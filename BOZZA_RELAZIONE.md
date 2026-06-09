# Pipeline Big Data per il monitoraggio IoT ambientale e il rilevamento di incendi

### Bozza di relazione — Esame di Big Data (Laurea Magistrale)

> **Nota per il docente.** Questa è una **bozza** preliminare: l'obiettivo è
> sottoporre l'impostazione del progetto (obiettivi, stack, trattamento dei dati)
> per capire se l'impianto è adeguato e se conviene aggiungere/togliere qualcosa
> prima della versione finale. In coda alla relazione ho raccolto i **punti su cui
> chiedo conferma** e alcune **possibili estensioni**.

---

## 1. Obiettivo del progetto

Realizzare una **pipeline Big Data end-to-end, interamente containerizzata**, che
acquisisce in tempo reale i dati di **4 nodi sensore IoT ambientali**, li elabora
in streaming con **rilevamento di anomalie e di incendi**, e li rende disponibili
su **dashboard live** aggiornate ogni pochi secondi.

Il progetto è pensato per mettere in pratica i temi del corso su un caso realistico:

- **Ingestione in tempo reale** di flussi continui da sorgenti multiple e indipendenti;
- **Stream processing distribuito** con elaborazione stateful (statistiche online);
- **Architettura Lambda** con *speed layer* (streaming) e *batch layer* (analisi storica);
- **Persistenza poliglotta** (document store + motore di ricerca/time-series);
- **Tolleranza ai guasti** a livello di message broker;
- **Visualizzazione** e *alerting* sui dati elaborati.

Caso d'uso applicativo: **early warning di incendi** in ambiente boschivo a partire
da sensori di gas, fumo, temperatura e qualità dell'aria.

---

## 2. I dati

### 2.1 Origine e natura

I dati provengono da **acquisizioni reali** effettuate con nodi sensore fisici in
condizioni diverse: scenari *normali* e scenari di **combustione controllata** (es.
aghi di pino, pino/lentisco). Ogni nodo ha prodotto file CSV che vengono "rigiocati"
in streaming verso la pipeline, simulando un deployment in cui ogni nodo è una
macchina separata che pubblica le proprie letture.

I nodi sono 4 (`nodo_1 … nodo_4`), per un totale di **6 741 letture**
(rispettivamente 1 732 / 1 731 / 1 668 / 1 610 righe). Ogni nodo viene rigiocato
in loop continuo, quindi il flusso verso la pipeline è di fatto illimitato.

### 2.2 Struttura di una lettura

Ogni riga/messaggio contiene le grandezze ambientali misurate:

| Campo | Descrizione |
|---|---|
| `Temperature (C)` | Temperatura |
| `Humidity (%)` | Umidità relativa |
| `Pressure (hPA)` | Pressione |
| `Gas (Ohm)` | Resistenza del sensore di gas (cala in presenza di volatili/fumo) |
| `Visible Light`, `IR`, `UV index` | Luce visibile, infrarosso, indice UV |
| `CO`, `NO2` | Monossido di carbonio, biossido di azoto |
| `Smoke (ppm)` | Fumo |
| `Fire` | **Etichetta ground-truth** (0 = normale, ≥1 = incendio) |

Il campo `Fire` funge da **verità di riferimento** per validare il rilevamento.
È presente su `nodo_1/2/3` ma **assente su `nodo_4`**: questo nodo viene quindi
usato come caso "senza etichetta", su cui la detection lavora in modo non
supervisionato e che viene escluso dalla validazione.

### 2.3 Arricchimenti aggiunti dalla pipeline

In fase di pubblicazione ogni messaggio viene completato con:
`node_id` (nodo di provenienza), `reading_index` (indice progressivo di lettura) e
`ingest_ts` (timestamp ISO‑8601 UTC, usato come *event-time* per le finestre).

---

## 3. Lo stack tecnologico

| Ruolo | Tecnologia | Versione |
|---|---|---|
| Message broker | **Apache Kafka** (cluster KRaft, 3 broker, senza ZooKeeper) | 7.6.1 |
| Stream processing | **Apache Spark** Structured Streaming (Standalone: 1 master + 3 worker) | 3.5.3 |
| Document store | **MongoDB** | 7.0 |
| Time-series / search | **Elasticsearch** | 7.17 |
| Dashboard | **Grafana** | 10.4 |
| Producer | **Python** + `kafka-python` | 3.11 |
| Orchestrazione | **Docker Compose** (14 servizi) | — |

L'intero stack gira in locale tramite Docker Compose, ma è **progettato come se
fosse multi-macchina**: ogni nodo sensore è un container distinto, Spark è un
cluster vero (master + 3 worker), Kafka è un cluster a 3 broker, e i servizi
comunicano per *hostname* come farebbero su host fisici separati.

---

## 4. Architettura della pipeline

```
[macchina 1] producer nodo_1 ─┐
[macchina 2] producer nodo_2 ─┤   Kafka cluster            Spark Standalone
[macchina 3] producer nodo_3 ─┼─► (3 broker, 4 part.,  ──► (1 master + 3 worker)
[macchina 4] producer nodo_4 ─┘    repl-factor 3)             │
                                                              ▼
                                        ┌─────────────────────┴───────────────────┐
                                   MongoDB                                  Elasticsearch
                            (raw / processed /                        (time-series, stato live,
                             stats / fire / batch)                     finestre, baseline)
                                        └─────────────────────┬───────────────────┘
                                                              ▼
                                                          Grafana
```

**Flusso ad alto livello:**

1. **4 producer Python** leggono i CSV dei rispettivi nodi e pubblicano le letture
   come messaggi JSON su Kafka. Ogni nodo scrive su una **partizione dedicata**
   (0–3): così l'ordine di lettura per nodo è garantito.
2. **Kafka** (topic `iot.sensor.data`, 4 partizioni, *replication-factor* 3,
   `min.insync.replicas` 2) bufferizza i messaggi e garantisce la tolleranza ai
   guasti: il cluster sopravvive alla perdita di un broker senza perdere dati.
3. **Spark Structured Streaming** consuma in **micro-batch ogni 5 secondi**,
   elabora e arricchisce i dati e scrive su più *sink*.
4. **MongoDB ed Elasticsearch** persistono i dati grezzi, elaborati e aggregati.
5. **Grafana** interroga Elasticsearch e mostra dashboard live (aggiornamento ogni 5 s).

---

## 5. Come tratto i dati

Il cuore del progetto è il job Spark. Per ogni micro-batch:

### 5.1 Ingestione e storico grezzo
Il batch viene prima scritto **così com'è** su MongoDB (`raw_readings`): è lo storico
immutabile, base per qualunque rielaborazione futura (con TTL per non crescere
all'infinito).

### 5.2 Pulizia (cleaning)
Filtro degli **outlier fisicamente impossibili** (es. `CO > 1000 ppm`,
`Gas > 1 MΩ`): valori palesemente errati del sensore, scartati prima dell'analisi.

### 5.3 Arricchimento e rilevamento anomalie
Per ogni nodo (l'elaborazione stateful è **distribuita sui worker**, una partizione
= un nodo, via `foreachPartition`) calcolo:

- **Z-score online (algoritmo di Welford)** su temperatura, CO, fumo e gas. La media
  e la varianza vengono aggiornate **incrementalmente**, senza conservare lo storico,
  e lo stato `{count, mean, m2}` è persistito su MongoDB così da **sopravvivere ai
  riavvii**. Lo z-score è *firmato* (positivo per picchi, negativo per cali → utile a
  distinguere un'impennata da un guasto). Anomalia se `|z| > 2σ`.
- **Soglie assolute** (CO > 50 ppm, fumo > 0.08, temperatura > 35 °C, gas < 5 kΩ),
  tarate sui dati reali. Servono a coprire il *warm-up* dello z-score (quando c'è
  poca storia) e i casi con baseline strutturalmente alta.

I due meccanismi lavorano **in parallelo e in modo complementare**.

### 5.4 Logica di transizione "fire"
Un **evento incendio** viene registrato **solo nella transizione** da
`Fire = 0` a `Fire ≥ 1` (non a ogni riga in cui `Fire ≥ 1`): questo evita di
inondare la collezione di eventi ridondanti. Il valore precedente è persistito per
nodo. (Opzionale, già predisposto: invio di un **alert via webhook** in modo
asincrono, fuori dal percorso critico del micro-batch.)

### 5.5 Aggregazioni a finestra
Una **seconda query streaming indipendente** (proprio reader e checkpoint) calcola
aggregati su **finestre temporali tumbling di 1 minuto** con *watermark* di 2 minuti
(media/max/min dei sensori per nodo), scritti su un indice dedicato. È l'esempio
canonico di *event-time windowing* con gestione dei dati in ritardo.

### 5.6 Persistenza (architettura Lambda)

| Layer | Destinazione | Contenuto |
|---|---|---|
| Speed — raw | MongoDB `raw_readings` | Ogni messaggio, pre-filtro |
| Speed — processed | MongoDB `processed_readings` | Dati arricchiti (z-score, flag) |
| Serving — time-series | ES `sensors_live_index` | Serie storiche per Grafana |
| Serving — stato live | ES `node_status_index` | 1 documento per nodo (stato corrente) |
| Serving — finestre | ES `window_stats` | Aggregati a finestra |
| Stato | MongoDB `node_stats`, `agg_per_nodo` | Welford + rolling stats cumulative |
| Eventi | MongoDB `fire_events` | Solo transizioni di incendio |

### 5.7 Batch layer storico
Un **job Spark batch separato** (`batch_analytics.py`) rilegge l'intera
`raw_readings` e calcola, in modo on-demand, analisi pesanti che lo streaming **non
può** fare (richiedono l'intero dataset):

- **percentili** (p50/p95/p99), baseline e trend orari per nodo;
- **matrice di correlazione di Pearson** tra i sensori (con **Spark MLlib**), globale
  e per nodo → quali grandezze "si muovono insieme" durante la combustione;
- **validazione del rilevamento contro il ground-truth** `Fire`: confusion matrix e
  **precision / recall / F1 / accuracy** per nodo.

Lo snapshot risultante va su MongoDB (`node_baseline`) con mirror su Elasticsearch
per Grafana. Questo chiude l'**architettura Lambda**: *speed layer* (streaming a 5 s)
+ *batch layer* (analisi a freddo sull'intero storico).

---

## 6. Visualizzazione

Le dashboard Grafana sono **provisioning-as-code** (definite da file, ricreate
automaticamente):

- **IoT Sensor Dashboard** — vista globale sui 4 nodi: card di stato FIRE / NO FIRE,
  serie storiche di temperatura/CO/fumo, conteggio anomalie, attività incendi.
- **IoT Node Detail** — drill-down su singolo nodo: tutti i sensori sullo stesso asse
  temporale, andamento z-score, timeline anomalie, statistiche cumulative.
- **Mappa Incendi** — mappa geografica dei 4 nodi (pannello Geomap su basemap
  OpenStreetMap). I marker cambiano colore in tempo reale in base allo stato del nodo
  (verde = normale, arancione = anomalia, rosso = incendio), aggiornati ogni 5 s.
  Cliccando un nodo si apre un riepilogo sintetico dei suoi dati e un link al dettaglio
  del singolo nodo. *Nota metodologica*: i sensori reali non hanno GPS, quindi le
  coordinate dei nodi sono **fittizie**, assegnate a mano in una zona boschiva a est di
  Cagliari (massiccio dei Sette Fratelli); il basemap, invece, è reale. Tecnicamente la
  mappa riusa l'indice di stato `node_status_index` (1 documento per nodo) arricchito
  con `lat`/`lon` e un campo di livello-stato.
- (Bozza) **Historical Dashboard** — output del batch layer: percentili, correlazioni,
  metriche di validazione.

---

## 7. Aspetti "Big Data" enfatizzati

- **Sorgenti distribuite e parallele**: 4 producer indipendenti, una partizione Kafka
  ciascuno.
- **Elaborazione distribuita reale**: cluster Spark con 3 worker; la logica stateful
  per nodo gira **sugli executor**, non sul driver (nessun `collect()` sui dati di
  dettaglio).
- **Tolleranza ai guasti**: replication-factor 3 + `min.insync.replicas` 2 → il
  cluster Kafka regge la perdita di un broker (dimostrabile dal vivo fermando un
  broker mentre la pipeline continua a ingerire).
- **Stato persistente e ripartenza**: lo stato Welford e gli offset Kafka
  (checkpoint Spark) sopravvivono ai riavvii.
- **Persistenza poliglotta**: MongoDB come document store / storico, Elasticsearch
  come motore time-series per le dashboard.

---

## 8. Stato attuale del progetto

**Già funzionante:**
- Cluster Kafka a 3 broker, 4 producer, cluster Spark, MongoDB, Elasticsearch, Grafana, il tutto orchestrato con Docker Compose;
- Pipeline streaming completa (cleaning → z-score Welford → soglie → fire transition → multi-sink);
- Seconda query a finestra;
- Batch layer storico con percentili, correlazioni MLlib e validazione precision/recall;
- Dashboard live provisioning-as-code;
- Mappa geografica live (Geomap) con marker per livello di stato, popup dei dati del nodo e drill-down al singolo nodo.

**Da rifinire prima della versione finale:**
- Consolidare e documentare i risultati della validazione (numeri di precision/recall per nodo);
- Completare la dashboard storica (batch layer);
- Sezione di valutazione sperformance/throughput.

---

## 9. Punti su cui chiedo conferma al docente

1. **Ampiezza dello stack**: l'insieme Kafka + Spark + MongoDB + Elasticsearch +
   Grafana è considerato adeguato e bilanciato per l'esame, o è preferibile
   approfondire di più un singolo componente piuttosto che coprirne molti?
2. **Anomaly detection**: l'approccio (z-score online di Welford + soglie assolute)
   è sufficiente, oppure ci si aspetta un **modello di ML vero e proprio** (es.
   classificatore allenato sul `Fire`, clustering, Isolation Forest)?
3. **Architettura Lambda**: la separazione speed/batch è apprezzata o
   sovradimensionata per la quantità di dati in gioco? Avrebbe senso citare/usare
   un'impostazione **Kappa** come alternativa?
4. **Dati**: ~6 700 letture reali sono sufficienti come volume, o conviene
   aumentare/sintetizzare il dataset per dare più sostanza alla parte "big"?
5. **Validazione**: le metriche scelte (precision/recall/F1/accuracy vs ground-truth
   `Fire`) sono il taglio giusto per dimostrare la bontà del rilevamento?

## 10. Possibili estensioni (se richiesto)

- **Modello ML supervisionato** sul `Fire` con valutazione train/test (Spark MLlib).
- **Alerting** end-to-end (il webhook è già predisposto) con notifica reale.
- **Benchmark** di throughput/latenza al variare del numero di worker/partizioni.
- **Kibana** o pannelli aggiuntivi per l'analisi esplorativa su Elasticsearch.
- **Schema Registry** (Avro) per la governance dei messaggi Kafka.

---

*Il codice e la configurazione completa (Docker Compose, job Spark, dashboard) sono
disponibili nel repository a corredo di questa bozza.*
