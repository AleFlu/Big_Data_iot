# Relazione Tecnica — Sistema IoT Big Data Streaming

## Indice

1. [Architettura generale](#1-architettura-generale)
2. [Tecnologie scelte e motivazioni](#2-tecnologie-scelte-e-motivazioni)
3. [Pipeline dati: dal CSV a Grafana](#3-pipeline-dati-dal-csv-a-grafana)
4. [Trasformazioni e arricchimento dei dati](#4-trasformazioni-e-arricchimento-dei-dati)
5. [Anomaly detection: soglie e algoritmi](#5-anomaly-detection-soglie-e-algoritmi)
6. [Persistenza: struttura dei dati in MongoDB ed Elasticsearch](#6-persistenza-struttura-dei-dati-in-mongodb-ed-elasticsearch)
7. [Visualizzazione in Grafana](#7-visualizzazione-in-grafana)
8. [Deployment multi-macchina simulato](#8-deployment-multi-macchina-simulato)
9. [Distribuzione del calcolo e scalabilità](#9-distribuzione-del-calcolo-e-scalabilità)

---

## 1. Architettura generale

Il sistema implementa una pipeline di analisi **streaming live** per una rete di 4 sensori IoT che trasmettono dati ambientali in tempo reale. L'obiettivo è rilevare anomalie e incendi e renderli visibili su dashboard aggiornate in continuo.

Il sistema simula un **deployment distribuito su 4 macchine distinte**: ogni nodo sensore ha il proprio processo producer indipendente, e Spark gira come cluster Standalone con 1 master e 3 worker separati — ognuno in un proprio container, come se fossero host fisici diversi.

```
[macchina-1]  csv-producer-1 (nodo_1, partizione 0) ──┐
[macchina-2]  csv-producer-2 (nodo_2, partizione 1) ──┤──▶ kafka:29092 ──▶ spark-master
[macchina-3]  csv-producer-3 (nodo_3, partizione 2) ──┤                        │
[macchina-4]  csv-producer-4 (nodo_4, partizione 3) ──┘               ┌────────┴────────┐
                                                              spark-worker-1  spark-worker-2
                                                              spark-worker-3
                                                                        │
                                                              spark-job (driver, client mode)
                                                                        │
                                              ┌─────────────────────────┴──────────────────────────┐
                                              │                                                      │
                                     ┌────────▼────────┐                              ┌─────────────▼──────┐
                                     │    MongoDB       │                              │  Elasticsearch 7.x │
                                     │  raw_readings    │◄── pre-filtro               │ sensors_live_index │
                                     │  processed_rdgs  │◄── post-elab. (batch layer) │ node_status_index  │
                                     │  node_stats      │                              └────────────────────┘
                                     │  fire_events     │                                        │
                                     │  agg_per_nodo    │                                        ▼
                                     └──────────────────┘
                                                                                        ┌──────────────────┐
                                                                                        │     Grafana       │
                                                                                        │  (2 dashboard     │
                                                                                        │   live)           │
                                                                                        └──────────────────┘
```

Tutti i servizi girano in container Docker sulla stessa rete `iot_net` (bridge). Il deployment è pensato per MacBook con architettura ARM64 (Apple Silicon).

---

## 2. Tecnologie scelte e motivazioni

### 2.1 Kafka (Confluent CP 7.6.1, KRaft mode) — cluster a 3 broker

Kafka è il bus centrale del sistema: disaccoppia i producer (CSV) dal consumer (Spark) garantendo che nessun messaggio venga perso anche se Spark si riavvia. La scelta di **KRaft** (Kafka senza ZooKeeper) elimina un intero servizio extra: in modalità KRaft sono gli stessi broker a formare il **quorum di controller**, senza un servizio di coordinamento separato.

**Cluster a 3 broker.** Kafka è deployato come **cluster di 3 broker** (`kafka`, `kafka2`, `kafka3`), ciascuno con ruolo `broker,controller`. I tre nodi condividono lo stesso `CLUSTER_ID` e lo stesso `KAFKA_CONTROLLER_QUORUM_VOTERS` (`1@kafka:9093,2@kafka2:9093,3@kafka3:9093`): formano un quorum di controller a 3 votanti che tollera la perdita di 1 nodo. Questa è la differenza fondamentale rispetto a un singolo broker: si ottiene **partitioning *e* replication** (le due dimensioni della distribuzione di Kafka), non solo il partizionamento.

**Replicazione e fault tolerance.** Il topic `iot.sensor.data` ha **4 partizioni** con **replication-factor 3** e **`min.insync.replicas=2`**: ogni partizione ha 3 repliche su broker distinti, e una scrittura è confermata solo quando almeno 2 repliche l'hanno persistita. Anche i topic interni (`__consumer_offsets`, transaction log) hanno RF=3. Il sistema **sopravvive alla perdita di un broker**: i leader delle partizioni che risiedevano sul broker caduto vengono rieletti automaticamente tra le repliche in-sync (ISR), e la pipeline continua a produrre e consumare senza interruzione (verificato fermando un broker a runtime: leader rieletti, ISR ridotta a 2, nessuna perdita di dati; al riavvio del broker le repliche si ri-sincronizzano e l'ISR torna a 3). Tutti i client (4 producer, Spark, Kafka UI) elencano i 3 broker come `bootstrap.servers`, così la caduta di un broker non impedisce loro di trovare gli altri.

**Partizionamento semantico.** Ogni producer scrive su una **partizione esplicita** (env var `KAFKA_PARTITION`: 0→nodo_1 … 3→nodo_4) — una *semantic partition function*, non il round-robin di default. Questo garantisce la distribuzione uniforme 1 nodo = 1 partizione indipendentemente dall'hash della chiave. Senza assegnazione esplicita, l'algoritmo murmur2 di kafka-python avrebbe mappato nodo_1 e nodo_4 sulla stessa partizione lasciando una partizione vuota. La chiave del messaggio resta `node_id`, che mantiene l'ordinamento per nodo all'interno della partizione.

> **Nota di dimensionamento.** 3 broker su un singolo laptop danno ridondanza (fault tolerance), non throughput aggiuntivo: a ~8 msg/s il collo di bottiglia non è mai il broker. In un deployment reale i 3 broker girerebbero su host separati. La replicazione del *dato applicativo* è completa; l'unico punto non ridondato è l'accesso *dall'host esterno* (porta 9092), esposto solo dal broker 1 — i client interni alla rete Docker usano invece tutti e 3 i broker.

### 2.2 Spark Structured Streaming 3.5.3 — Standalone Cluster

Spark gestisce il processing in modalità **micro-batch** (trigger ogni 5 secondi). La scelta di Spark rispetto ad alternative più leggere (Faust, Flink) è giustificata dalla necessità di:

- avere un'astrazione di streaming robusta con **checkpoint** e recupero automatico dello stato dopo i riavvii
- usare l'API DataFrame standard (`DataFrame`, `groupBy`, `agg`) coerente con il modello relazionale dei dati
- disporre di un percorso di crescita verso il calcolo realmente distribuito (state store per chiave, parallelismo per partizione) senza cambiare framework

Il sistema usa uno **Spark Standalone Cluster** con 1 Master e 3 Worker, per simulare un deployment distribuito reale. Il driver gira in modalità `client` nel container `spark-job`, separato dai worker. Il checkpoint è su un volume Docker condiviso tra tutti i container Spark, necessario perché in cluster mode il driver scrive il checkpoint su filesystem accessibile anche ai worker.

> Nota sulla distribuzione effettiva del calcolo: l'arricchimento stateful per nodo (Welford, z-score, flag) è **distribuito sugli executor** tramite `foreachPartition` con ripartizionamento per `node_id`, senza `collect()` sul driver. Il dettaglio del meccanismo e i percorsi di ulteriore scalabilità sono discussi nella **sezione 9**.

La stessa immagine Docker copre tutti i ruoli (master, worker, driver): la variabile d'ambiente `SPARK_ROLE` fa il dispatch nell'entrypoint.

### 2.3 Elasticsearch 7.17.28

ES è il datastore per la visualizzazione in tempo reale. È stato scelto rispetto a InfluxDB o PostgreSQL perché:

- Grafana ha un plugin ES nativo e maturo
- il mapping esplicito consente di tipizzare `ingest_ts` come `date` (necessario per Grafana time-series)
- le query KQL di Grafana sono semplici e potenti anche senza conoscere SQL

**Versione 7.17.x** (e non 8.x): il plugin Grafana per ES non supportava pienamente ES 8.x al momento della progettazione, e la versione 7.17 è la più recente della serie 7 con patch di sicurezza.

### 2.4 MongoDB 7.0

MongoDB viene usato per la **persistenza a lungo termine** e per le statistiche incrementali, ruoli per cui non è adatto ES (aggiornamenti parziali costosi, no `$inc`/`$min`/`$max` atomici). In particolare:

- `raw_readings`: archivio completo di ogni messaggio ricevuto (storico)
- `node_stats`: stato Welford per ogni nodo (aggiornato ogni batch, necessita upsert atomico)
- `fire_events`: registro eventi incendio (scrittura idempotente con ReplaceOne)
- `agg_per_nodo`: statistiche cumulative per nodo (min, max, sum, count con operatori atomici MongoDB)

### 2.5 Grafana 10.4.2

Grafana è stato scelto al posto di Kibana per:

- **provisioning dichiarativo** via file YAML/JSON: le dashboard si ricreano automaticamente a ogni restart senza configurazione manuale
- supporto a **template variables** con dropdown interattivi (selezione nodo)
- possibilità di aggiungere plugin e datasource multipli dallo stesso container

### 2.6 Python (kafka-python, elasticsearch-py, pymongo)

Il connettore ES per Spark (`elasticsearch-spark-30`) è incompatibile con Spark 3.5.x a causa di un bug confermato nel Catalyst optimizer (GitHub issue #2210: `NoSuchMethodError`). La soluzione adottata è scrivere su ES direttamente in Python con la libreria ufficiale `elasticsearch==7.17.9`, all'interno della logica `foreachBatch`/`foreachPartition` (le scritture time-series e di stato avvengono sugli executor, vedi sezione 9). Questo approccio elimina il conflitto JAR ed è pienamente funzionale.

---

## 3. Pipeline dati: dal CSV a Grafana

### 3.1 Sorgente: CSV Producer ×4

Quattro istanze indipendenti del producer, una per nodo, ciascuna in un container separato. Ogni istanza legge solo il proprio CSV, identificato dalle variabili d'ambiente `NODE_ID` e `CSV_PATH`. Questo simula 4 macchine fisiche distinte che raccolgono e trasmettono dati in modo autonomo.

I file CSV provengono da sensori fisici BME680 + MQ-x con caratteristiche distinte:

| Nodo | Righe | Schema | Caratteristica |
|------|-------|--------|----------------|
| nodo_1 | 1.733 | 11 col (con Fire) | ~44% normale, ~56% fire=1 |
| nodo_2 | 1.732 | 11 col (con Fire) | 100% fire=1 (acquisizione con accendino) |
| nodo_3 | 1.669 | 11 col (con Fire) | ~25% normale, ~25% fire=1, ~50% fire=2 |
| nodo_4 | 1.611 | 10 col (senza Fire) | sensore ambientale puro, nessun campo Fire |

Ogni producer esegue un loop continuo sul proprio CSV (`LOOP_FOREVER=true`). Il `reading_index` è un contatore **globale monotono crescente** che si incrementa a ogni messaggio su tutti i pass, non un indice relativo al CSV: questo garantisce che ogni messaggio abbia un identificatore univoco anche in modalità loop, evitando collisioni nelle chiavi upsert di MongoDB.

Per ogni messaggio inviato il producer aggiunge:
- `node_id` (string): identificatore del nodo (`nodo_1` ... `nodo_4`)
- `reading_index` (int): contatore globale crescente — univoco per tutta la sessione
- `ingest_ts` (string ISO8601 UTC): timestamp reale al momento dell'invio

La chiave Kafka è il `node_id`. La partizione è assegnata esplicitamente (0→nodo_1, 1→nodo_2, 2→nodo_3, 3→nodo_4) per garantire distribuzione uniforme.

### 3.2 Kafka: trasporto e buffering

Il topic `iot.sensor.data` riceve i messaggi serializzati in JSON. Kafka agisce da **buffer disaccoppiante**: se Spark è lento o si riavvia, i messaggi restano nel topic (retention 24 ore) e vengono recuperati dal checkpoint. La configurazione `acks="all"` nel producer garantisce che il messaggio sia persistito sul broker prima di procedere al successivo.

Il consumo avviene dall'offset `earliest` con un limite di `maxOffsetsPerTrigger=200` per batch: evita che al primo avvio (con la coda già piena di messaggi storici) Spark processi tutto in un batch gigante.

### 3.3 Spark: parsing e processing

Ogni batch viene processato nella callback `foreachBatch(process_batch, batch_id)`.

**Parsing JSON**: il payload Kafka (campo `value`, bytes) viene castato a stringa e deserializzato con `from_json` usando lo schema `SENSOR_SCHEMA` dichiarato esplicitamente. I campi numerici sono `DoubleType` (non `FloatType`: JSON deserializza i numeri come double di default e la conversione implicita introduce errori di arrotondamento). `Fire` è `IntegerType` con `nullable=True` per gestire nodo_4.

**Forma dei dati in ingresso** (messaggio Kafka, campi originali CSV):

```json
{
  "Temperature (C)": 24.5,
  "Humidity (%)": 61.2,
  "Pressure (hPA)": 1013.1,
  "Gas (Ohm)": 52340.0,
  "Visible Light": 120.0,
  "IR": 871.0,
  "UV index": 0.01,
  "CO": 2.3,
  "NO2": 0.12,
  "Smoke (ppm)": 0.024,
  "Fire": 0,
  "node_id": "nodo_1",
  "reading_index": 142,
  "ingest_ts": "2026-05-23T09:15:10.487Z"
}
```

Il passaggio da questa forma al documento ES finale avviene tramite le trasformazioni descritte nella sezione successiva (filtro outlier, rinomina campi, arricchimento con campi calcolati).

### 3.4 Elasticsearch: indici time-series e stato

Spark scrive su due indici distinti:

- **`sensors_live_index`**: serie temporale — un documento per ogni messaggio processato, con tutti i campi rinominati in snake_case e i campi calcolati (z-score, is_anomaly, is_fire). Ogni documento ha `_id = node_id_reading_index`, che rende la scrittura **idempotente**: un retry di Spark su un batch fallito sovrascrive i documenti esistenti invece di duplicarli.
- **`node_status_index`**: stato corrente — esattamente 4 documenti (uno per nodo), upsertati a ogni batch con l'ultima lettura. Usato dai pannelli "stato nodo" sulla home dashboard.

### 3.5 MongoDB: storico e statistiche

In parallelo a ES, MongoDB riceve:
- copia raw di ogni record (pre-filtro) in `raw_readings`
- registro degli eventi fire in `fire_events`
- statistiche aggregate cumulative in `agg_per_nodo`

### 3.6 Grafana: visualizzazione

Grafana interroga ES ogni 5 secondi (auto-refresh) e mostra:
- **Home dashboard**: stato di ciascun nodo (fire/OK), grafici temporali per temperatura, CO, smoke, gas
- **Node Detail dashboard**: dropdown per selezionare il nodo, tutti i parametri del nodo scelto, z-score, tabella ultimi record

---

## 4. Trasformazioni e arricchimento dei dati

### 4.1 Filtro outlier

Prima di qualsiasi analisi vengono scartati i record con valori fisicamente impossibili (saturazione o disconnessione del sensore):

```
CO       > 1000 ppm   → scartato (sensore difettoso o saturazione)
Gas (Ohm) > 1.000.000 → scartato (sensore disconnesso)
```

I record scartati vengono comunque scritti su MongoDB `raw_readings` (prima del filtro), per conservare la traccia completa.

### 4.2 Rinomina campi per Elasticsearch

I nomi originali dei CSV contengono spazi, parentesi e simboli non compatibili con KQL (il linguaggio di query di Grafana). Tutti i campi vengono rinominati in snake_case prima della scrittura su ES:

| Campo originale | Campo ES |
|----------------|----------|
| `Temperature (C)` | `temperature_c` |
| `Humidity (%)` | `humidity_pct` |
| `Pressure (hPA)` | `pressure_hpa` |
| `Gas (Ohm)` | `gas_ohm` |
| `Visible Light` | `visible_light` |
| `IR` | `ir` |
| `UV index` | `uv_index` |
| `CO` | `co` |
| `NO2` | `no2` |
| `Smoke (ppm)` | `smoke_ppm` |
| `Fire` | `fire` |

I nomi originali restano invariati in MongoDB, dove vengono scritti i dati raw.

### 4.3 Campi calcolati aggiunti

Ogni documento che entra in ES viene arricchito con questi campi calcolati:

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `zscore_Temperature` | float | Z-score online firmato (Welford) per temperatura |
| `zscore_CO` | float | Z-score online firmato per CO |
| `zscore_Smoke` | float | Z-score online firmato per smoke |
| `zscore_Gas` | float | Z-score online firmato per gas |
| `is_anomaly` | boolean | `true` se almeno una soglia (`|z| > 2` o assoluta) è superata |
| `anomaly_sensors` | keyword | Lista sensori anomali (es. `"CO, Temperature"`) |
| `is_fire` | boolean | `true` se `Fire >= 1` |
| `is_fire_transition` | boolean | `true` solo nel passaggio no-fire → fire (`0/None` → `>=1`) |
| `fire_state_label` | keyword | `"NORMAL"`, `"FIRE"`, `"SPECIAL"`, `"N/A"` |

Lo **z-score è firmato**: positivo quando il valore è sopra la media storica del nodo, negativo quando è sotto. Questo permette di distinguere visivamente un'impennata (es. picco di temperatura) da un crollo (es. guasto o disconnessione del sensore) nei grafici Grafana. La classificazione di anomalia avviene sul **valore assoluto** (`|z| > 2`), quindi è simmetrica nelle due direzioni.

---

## 5. Anomaly detection: soglie e algoritmi

Il sistema usa due meccanismi complementari che si integrano: uno statistico adattivo e uno basato su soglie fisiche assolute. Un'anomalia viene segnalata se **almeno uno** dei due meccanismi scatta.

### 5.1 Algoritmo Welford (z-score online)

L'algoritmo di Welford calcola media e varianza in modo **incrementale**, senza dover tenere in memoria tutti i valori precedenti. Per ogni nodo e per ogni sensore monitorato (Temperature, CO, Smoke, Gas), viene mantenuto uno stato `{count, mean, m2}` persistito in MongoDB `node_stats`.

**Aggiornamento per ogni nuovo valore `x`:**
```
n    = count + 1
δ    = x - mean_old
mean = mean_old + δ/n
δ2   = x - mean_new
m2   = m2_old + δ × δ2
std  = sqrt(m2 / (n-1))     ← varianza campionaria (non di popolazione)
z    = (x - mean) / std     ← z-score firmato (segno = direzione dello scostamento)
```

Lo z-score misura quante deviazioni standard il valore corrente si allontana dalla media storica del **nodo specifico**. È **firmato**: positivo sopra la media, negativo sotto, così i grafici distinguono un picco da un crollo. La classificazione di anomalia usa il valore assoluto: `|z| > 2`. La soglia di 2σ include il 95.4% dei valori "normali" in una distribuzione gaussiana, quindi il 4.6% dei punti normali viene segnalato come falso positivo — accettabile per un sistema di early warning. I pannelli Grafana z-score mostrano le bande di soglia simmetriche a `+2` e `-2`.

La varianza **campionaria** (divisa per `n-1`, formula di Bessel) è preferita a quella di popolazione (divisa per `n`) per correggere il bias di sottostima nelle prime letture, quando `n` è piccolo.

**Warm-up**: nelle prime decine di letture, la media e la deviazione standard sono poco stabili. Le soglie assolute (sezione 5.2) compensano questo periodo di warm-up.

### 5.2 Soglie assolute

Le soglie assolute scattano indipendentemente dallo z-score e sono ancorate ai valori fisici misurati nei CSV reali:

| Sensore | Soglia | Motivazione |
|---------|--------|-------------|
| CO | > 50 ppm | La baseline normale è 1-5 ppm. Sopra 50 ppm c'è combustione attiva (nodo_3 con fire=1/2 raggiunge 993 ppm). La soglia OSHA per esposizione lavorativa è 50 ppm/8h. |
| Smoke | > 0.08 ppm | Baseline normale 0.01-0.04 ppm. Durante eventi fire il nodo_2 raggiunge 0.18 ppm. La soglia a 0.08 è il doppio del massimo baseline: distingue rumore dal segnale. |
| Temperatura | > 35°C | Range normale 20-32°C dai dati. Sopra 35°C c'è riscaldamento anomalo (allineato alla soglia arancione su Grafana per coerenza visiva). |
| Gas | < 5 000 Ohm | Il sensore BME680 misura una **resistenza** che *cala* in presenza di composti volatili (fumo, gas combusti): baseline ~10k-200k Ohm, una caduta sotto 5 kΩ segnala alta concentrazione. È l'unica soglia con verso *minore-di*, perché qui il segnale di rischio è una diminuzione, non un aumento. |

Le soglie assolute coprono **tutti e quattro i sensori** (CO, Smoke, Temperatura, Gas) e hanno due ruoli specifici che lo z-score non copre:

1. **Early detection / warm-up**: alla prima lettura di un sensore `n=1`, quindi la deviazione standard campionaria è indefinita (`std=0`) e lo z-score è forzato a 0. In questa fase solo le soglie assolute rilevano eventi pericolosi. Coprendo tutti e 4 i sensori, nessuno resta scoperto durante il warm-up.
2. **Baseline alta**: se un nodo ha una baseline strutturalmente alta (es. nodo_2 sempre in fire), tutti i valori alti hanno z-score basso — le soglie assolute garantiscono comunque il rilevamento.

I flag prodotti dai due meccanismi usano gli stessi nomi (`CO`, `Smoke`, `Temperature`, `Gas`) per coerenza: il campo `anomaly_sensors` in ES è filtrabile in Grafana senza distinzione di origine (z-score o soglia assoluta).

### 5.3 Flag Fire (sensore dedicato)

Il campo `Fire` nel CSV è prodotto da un sensore dedicato (MQ-2 o equivalente con soglia hardware) e vale:
- `0` → normale
- `1` → rilevazione incendio
- `2` → rilevazione incendio ad alta intensità (solo nodo_3)
- `null` → sensore non presente (nodo_4)

Il flag `is_fire = (Fire >= 1)` è **indipendente** dall'anomaly detection: un evento fire può non essere un'anomalia statistica (es. nodo_2 è sempre a fire=1, quindi lo z-score è basso), e viceversa un'anomalia può non essere un incendio (es. picco di temperatura senza fire).

---

## 6. Persistenza: struttura dei dati in MongoDB ed Elasticsearch

### 6.1 Elasticsearch — `sensors_live_index`

Un documento per ogni messaggio processato. Usato da Grafana per le serie temporali.

**Campi chiave:**
- `ingest_ts` (`date`): campo temporale — indispensabile per Grafana
- `node_id` (`keyword`): per filtrare per nodo
- tutti i sensori rinominati in snake_case (`float`)
- `zscore_*` (`float`): z-score per i 4 sensori monitorati
- `is_anomaly` (`boolean`), `anomaly_sensors` (`keyword`)
- `is_fire` (`boolean`), `fire` (`integer`), `fire_state_label` (`keyword`)

Il mapping è **creato esplicitamente** alla prima scrittura con `refresh_interval: 1s` e `number_of_replicas: 0` (cluster single-node, le repliche non hanno senso). Senza mapping esplicito ES avrebbe inferito `ingest_ts` come `text` e Grafana non avrebbe riconosciuto il campo temporale.

Ogni documento ha `_id = {node_id}_{reading_index}`. Questo rende le scritture **idempotenti**: se Spark deve reprocessare un batch (restart, failure), i documenti ES vengono sovrascritti invece di duplicati. In combinazione con il `reading_index` globale crescente (non relativo al CSV), ogni messaggio ha un `_id` univoco per tutta la durata della sessione.

### 6.2 Elasticsearch — `node_status_index`

Quattro documenti fissi (uno per nodo), aggiornati a ogni batch. Il documento ha `_id = node_id`, quindi ogni upsert sovrascrive il documento precedente. Usato per i pannelli "stato corrente" della home dashboard.

Contiene: ultima lettura del sensore, z-score correnti, `is_fire`, `is_anomaly_current`, `last_update_ts`.

### 6.3 MongoDB — `processed_readings`

Contiene i dati **post-elaborazione Spark**: ogni record è già filtrato (outlier rimossi), arricchito con z-score per i 4 sensori, flag `is_anomaly`, `anomaly_sensors`, `is_fire`, `fire_state_label`. I nomi dei campi sono quelli originali del CSV (non rinominati come in ES).

Questo è il **batch layer** del sistema in ottica Lambda Architecture: la source of truth per qualsiasi rianalisi futura. Se si vogliono ricalcolare le anomalie con soglie diverse, addestrare un modello ML sui dati storici, o rieseguire aggregazioni con parametri diversi, si parte da `processed_readings` — non da `raw_readings` (che non ha z-score) né da Elasticsearch (che è il serving layer, non l'archivio definitivo).

La scrittura è idempotente via `ReplaceOne` con chiave `(node_id, reading_index)`. Gli indici includono `is_anomaly` e `is_fire` per query analitiche efficienti sull'intero storico.

### 6.4 MongoDB — `raw_readings`

Archivio completo di ogni messaggio ricevuto, inclusi quelli filtrati come outlier. I nomi dei campi sono **originali** (con spazi e parentesi). Indici su `node_id`, `ingest_ts`, e composto `(node_id, ingest_ts)`. Scrittura con `operationType=replace` e `idFieldList=node_id,reading_index` per idempotenza (sicuro su retry Spark).

### 6.5 MongoDB — `node_stats`

Uno documento per nodo con lo stato Welford corrente: `{count, mean, m2}` per ciascuno dei 4 sensori monitorati. Aggiornato con `ReplaceOne(upsert=True)` a ogni batch. Serve esclusivamente al calcolo incrementale dello z-score — non viene visualizzato.

### 6.6 MongoDB — `fire_events`

Registro delle **transizioni** no-fire → fire, **non** di ogni record con `Fire >= 1`. Un evento viene scritto solo quando un nodo passa da `Fire = 0` (o `null`, mai visto prima) a `Fire >= 1`: lo stato precedente (`last_fire_value`) è persistito in `node_stats` per nodo. Questo evita di intasare la collezione con migliaia di righe per i nodi strutturalmente in fire (es. nodo_2, sempre a `Fire = 1`): senza questa logica ogni riga di nodo_2 genererebbe un evento.

La scrittura è idempotente via `ReplaceOne(filter={"node_id": ..., "reading_index": ...}, upsert=True)`: il `reading_index` globale crescente identifica univocamente il messaggio, quindi un retry di Spark sovrascrive l'evento invece di duplicarlo. Ogni documento contiene `fire_value`, `fire_value_prev`, temperatura, CO e smoke al momento della transizione per analisi forensi.

Opzionalmente, se la variabile d'ambiente `ALERT_WEBHOOK_URL` è valorizzata, ogni transizione fire invia anche un **alert HTTP POST** verso l'endpoint configurato (es. webhook.site, Slack, Discord). L'invio avviene in un **thread daemon separato**, fuori dal path critico del micro-batch: un endpoint lento o irraggiungibile non può rallentare lo streaming (il trigger è di 5 s). L'alerting è disabilitato lasciando la variabile vuota.

### 6.7 MongoDB — `agg_per_nodo`

Statistiche cumulative per nodo, aggiornate ogni batch con operatori atomici MongoDB:
- `$inc` per contatori e somme (calcolo della media cumulativa: `running_sum / total_processed`)
- `$min` / `$max` per i minimi e massimi storici — con guard esplicito su `None`: se tutti i record di un batch per un nodo hanno un sensore a `null`, il campo `$min`/`$max` viene escluso dall'update per evitare di resettare il minimo storico a `null`
- `$set` per gli ultimi valori medi del batch

---

## 7. Visualizzazione in Grafana

### 7.1 Home Dashboard

Mostra una visione globale della rete (11 pannelli):

- **4 pannelli stato nodo** (stat): leggono il campo `is_fire` dal `node_status_index` (`_id:nodo_X`, ultimo valore). Verde con testo "NO FIRE" se `false`/`null`, rosso "FIRE" se `true`. Nodo_4 mostra sempre "NO FIRE" (nessun sensore fire, `fire = null`).
- **Temperatura (°C) per Nodo** (time series): media per nodo, threshold gialla a 30°C e rossa a 35°C
- **CO per Nodo** (time series): scala lineare, threshold gialla a 50 ppm, arancione e rossa più in alto
- **Smoke (ppm) per Nodo** (time series separata da CO): scala con 4 decimali, threshold a 0.05/0.10/0.15 ppm. Smoke ha scala 0–0.35 ppm, incompatibile con CO (0–1000 ppm): accorparli renderebbe smoke una riga piatta.
- **Gas (Ohm) per Nodo** (time series): resistenza per nodo; threshold *invertita* (rosso sotto 5 kΩ, verde sopra 10 kΩ) perché un valore basso indica volatili/fumo.
- **Anomalie (ultimi 15 min)** (stat): conteggio globale dei documenti con `is_anomaly:true` nell'intervallo
- **Attività Fire nel Tempo per Nodo** (time series): conteggio per nodo dei documenti con `is_fire:true`
- **Ultimi Eventi Anomali** (table): tabella raw dei documenti con `is_anomaly:true`
- Link alla dashboard di dettaglio nodo (`IoT Node Detail`)

### 7.2 Node Detail Dashboard

Dashboard con dropdown `$node` per selezionare il nodo (nodo_1/2/3/4):

- Tutti i parametri del nodo selezionato: temperatura, umidità, pressione, CO, smoke, gas, NO2, IR, UV
- Stato fire corrente con label NORMAL/FIRE/SPECIAL/N/A
- 4 pannelli z-score (Temperature, CO, Smoke, Gas) con threshold **simmetrica** a `+2` e `-2` (z-score firmato: evidenzia sia i picchi sia i crolli)
- Tabella ultimi 50 record con color coding su `is_anomaly` e `is_fire`
- **Range storico (Lambda — Batch Layer)**: min/max storici di temperatura, CO, smoke e gas e numero totale di record processati, letti dai campi `running_*` del `node_status_index` (alimentati dalle aggregazioni cumulative di `agg_per_nodo`)

---

## 8. Deployment multi-macchina simulato

### 8.1 Strategia di simulazione

Il sistema simula un deployment distribuito su 4 macchine fisiche usando Docker Compose su un singolo host. Ogni componente è configurato come se girasse su un host separato: gli endpoint sono espliciti (`kafka:29092`, `spark://spark-master:7077`), non dipendono dalla co-locazione. Questo consente di passare a un deployment reale su macchine separate cambiando solo gli hostname nelle variabili d'ambiente.

Due componenti sono stati distribuiti:
1. **CSV Producer ×4** — 4 container indipendenti, ciascuno simula una macchina sensore
2. **Spark Standalone Cluster** — 1 master + 3 worker + 1 driver, ognuno in un container separato
3. **Kafka Cluster** — 3 broker KRaft (`kafka`, `kafka2`, `kafka3`), quorum di controller a 3 votanti, replication-factor 3 (vedi sezione 2.1)

I restanti componenti (Elasticsearch, MongoDB, Grafana) rimangono single-node: la complessità di distribuirli non è giustificata dal volume di dati (≈8 msg/sec totali). Kafka è invece clusterizzato perché la fault tolerance del bus di messaggi (replicazione delle partizioni) è il concetto Big Data centrale che il progetto vuole dimostrare, e il costo in RAM di 2 broker aggiuntivi (~1 GB) è sostenibile.

### 8.2 CSV Producer ×4

Ogni producer è parametrizzato via variabili d'ambiente:

| Env var | Descrizione |
|---------|-------------|
| `NODE_ID` | Identificatore del nodo (`nodo_1`..`nodo_4`) |
| `CSV_PATH` | Path assoluto al CSV di questo nodo |
| `KAFKA_PARTITION` | Partizione Kafka assegnata (0..3) |
| `PRODUCER_DELAY_MS` | Delay tra messaggi (default 500 ms) |

L'assegnazione esplicita della partizione Kafka risolve la collisione di hash: l'algoritmo murmur2 di kafka-python mappa `nodo_1` e `nodo_4` sulla stessa partizione, lasciando la partizione 2 sempre vuota. Con `KAFKA_PARTITION` ogni nodo è garantito sulla propria partizione.

### 8.3 Spark Standalone Cluster

Il cluster Spark è composto da 5 container che usano la stessa immagine Docker. Il ruolo è selezionato dalla variabile `SPARK_ROLE`:

| Container | `SPARK_ROLE` | Ruolo |
|-----------|-------------|-------|
| `spark-master` | `master` | Master del cluster — coordina worker e accept submit |
| `spark-worker-1/2/3` | `worker` | Executor — eseguono i task Spark |
| `spark-job` | `driver` | Driver — esegue `spark-submit` in client mode |

L'entrypoint usa uno `case` su `SPARK_ROLE` che lancia `spark-class` in foreground (non `start-master.sh`/`start-worker.sh` che sono script di background e causerebbero l'uscita immediata del container). Qualsiasi valore non riconosciuto produce un errore esplicito con `exit 1`.

**Submit in client mode**: il driver gira nel container `spark-job` e si connette al master su `spark://spark-master:7077`. La comunicazione inversa (master → driver per i risultati dei task) usa `spark.driver.host=spark-job`, che corrisponde all'hostname Docker del container.

**Checkpoint condiviso**: il volume `spark_checkpoints` è montato su tutti e 5 i container Spark. In Structured Streaming il checkpoint è scritto e letto solo dal driver, ma avere il volume disponibile su tutti i container garantisce la coerenza in caso di failover.

### 8.4 Limiti di memoria

Il sistema è dimensionato per un MacBook con Docker Desktop (VM da 8 GB):

| Servizio | mem_limit | Note |
|----------|-----------|------|
| Kafka | 512 MB | `-Xmx384m -Xms256m` |
| MongoDB | 512 MB | `--wiredTigerCacheSizeGB 0.25` |
| Elasticsearch | 1024 MB | `-Xms512m -Xmx512m` (entrambi obbligatori) |
| Grafana | 256 MB | — |
| spark-master | 512 MB | Solo coordinamento, nessun executor |
| spark-worker-1/2/3 | 640 MB × 3 | 512 MB heap executor + overhead JVM |
| spark-job (driver) | 768 MB | 512 MB driver heap + overhead |
| csv-producer-1/2/3/4 | 128 MB × 4 | Pure Python, ~50 MB effettivo per processo |
| mongo-express + kafka-ui | 384 MB | Tool di monitoring |
| **Totale** | **~6.4 GB** | Dentro il limite di 8 GB Docker VM |

ES richiede che `mem_limit` e `ES_JAVA_OPTS` siano allineati: senza `mem_limit` ES rileva tutta la RAM della Docker VM e alloca di conseguenza, causando OOM su altri container.

### 8.5 Checkpoint e idempotenza

Il checkpoint Spark è su volume Docker `spark_checkpoints:/spark/checkpoints`. Senza volume, un riavvio del container azzera il checkpoint e Spark ri-processa tutta la coda Kafka storica. Le scritture **per-record** sono idempotenti (un re-processing dello stesso micro-batch sovrascrive, non duplica):
- MongoDB `raw_readings`: `operationType=replace` + `idFieldList=node_id,reading_index`
- MongoDB `processed_readings`: `ReplaceOne(upsert=True)` con chiave `(node_id, reading_index)`
- MongoDB `fire_events`: `ReplaceOne(upsert=True)` con chiave `(node_id, reading_index)`
- ES `sensors_live_index`: `_id = node_id_reading_index` (upsert via bulk con `_id` esplicito)
- ES `node_status_index`: upsert con `_id = node_id`

**Limite noto — aggregati cumulativi non idempotenti.** Due strutture *stateful* fanno eccezione e **non** sono idempotenti su re-processing:
- `agg_per_nodo`: usa `$inc` su `total_processed` e `running_sum_*`. Se Spark ri-processa un micro-batch già applicato (recovery da failure a checkpoint disallineato), conteggi e somme cumulative vengono incrementati due volte, gonfiando la media cumulativa. I `$min`/`$max` restano corretti perché idempotenti per natura.
- `node_stats` (stato Welford): `count/mean/m2` accumulano sullo stato già persistito; un re-processing conterebbe due volte le stesse letture, distorcendo leggermente media e varianza da quel punto.

È una proprietà intrinseca degli **aggregati online**: renderli idempotenti richiederebbe di tracciare gli offset/`reading_index` già incorporati nello stato. L'impatto è limitato ai soli scenari di failure-recovery e ricade su **metriche di monitoraggio/storiche** (medie cumulative, range), **non** sulla detection in tempo reale (z-score e flag fire del singolo record restano corretti, essendo calcolati sul record e scritti in modo idempotente). In esercizio normale, senza fallimenti del driver, anche questi aggregati sono esatti.

### 8.6 Ordine di avvio

L'ordine di startup è vincolato da `depends_on` con healthcheck:

```
kafka (healthy) → kafka-init (completed) → csv-producer-1/2/3/4
                                         ↘
kafka + kafka-init + mongodb + elasticsearch (tutti healthy)
  → spark-master (healthy)
    → spark-worker-1/2/3 (started)
      → spark-job (driver, submit al cluster)
```

I worker aspettano che il master sia `healthy` (healthcheck su porta 8082) prima di registrarsi. Il driver aspetta che almeno i worker siano `started` per evitare di inviare il job a un cluster vuoto.

### 8.7 Sequenza di avvio manuale

```bash
cd "/Users/klay_flurry/Desktop/magistrale/Primo Semestre/Big_Data/Progetto"

# 1. Infrastruttura core
docker compose up -d kafka mongodb elasticsearch

# 2. Topic creation (aspetta kafka healthy, ~30-90s)
docker compose up kafka-init

# 3. Spark cluster
docker compose up -d spark-master
# Verifica: http://localhost:8082 → Spark Master UI
docker compose up -d spark-worker-1 spark-worker-2 spark-worker-3
# Verifica: UI deve mostrare "3 Workers, 3 Cores"

# 4. Spark job (submit al cluster)
docker compose up -d spark-job
docker compose logs -f spark-job  # aspetta "Streaming query started"

# 5. Producer ×4
docker compose up -d csv-producer-1 csv-producer-2 csv-producer-3 csv-producer-4

# 6. Tool di monitoring
docker compose up -d grafana mongo-express kafka-ui
```

---

## 9. Distribuzione del calcolo e scalabilità

Questa sezione analizza in modo critico **dove** viene eseguito il calcolo nel cluster Spark, quali parti sono realmente distribuite e come il sistema scalerebbe se il volume passasse dagli attuali ≈8 msg/s a ordini di grandezza superiori. La distribuzione del calcolo è il cuore di un sistema Big Data, quindi è il primo aspetto che documentiamo.

### 9.1 Cosa gira sui worker e cosa gira sul driver

La pipeline è progettata in modo che **l'intera logica stateful per-nodo sia distribuita sugli executor**, lasciando sul driver solo l'orchestrazione e le aggregazioni che richiedono una vista globale del batch. Il metodo `process_batch(batch_df, batch_id)` invocato da `foreachBatch` orchestra quattro passi:

| Passo | Operazione | Dove gira | Distribuito? |
|-------|-----------|-----------|--------------|
| 1 | Parsing JSON (`from_json`) + scrittura `raw_readings` (connettore Spark) | Worker (executor) | ✅ Sì, per partizione |
| 2 | Filtro outlier (`.filter(CO < … & Gas < …)`) + `cache()` | Worker | ✅ Sì, lazy |
| 3 | **Welford + z-score + arricchimento + scritture** (`processed_readings`, `sensors_live_index`, `node_stats`, `fire_events`, `node_status_index`) via **`foreachPartition`** | **Worker** | ✅ **Sì, per nodo** |
| 4 | Aggregazioni `groupBy("node_id").agg(...)` + upsert cumulativi `agg_per_nodo` | Worker (groupBy) + Driver (upsert atomici) | ✅ Sì |

Il punto chiave è il **passo 3**: non c'è alcun `collect()`. La logica di arricchimento riga-per-riga (la parte computazionalmente più pesante) viene eseguita **dentro le partizioni sugli executor**, mai sul driver. I dati arricchiti non tornano mai indietro: ogni worker scrive direttamente sui propri sink.

### 9.2 Come funziona la distribuzione per partizione

Il meccanismo si appoggia sull'allineamento tra partizionamento Kafka e partizionamento Spark:

1. **`repartition(F.col("node_id"))`** — prima di `foreachPartition`, il batch filtrato viene ripartizionato per `node_id` (hash partitioning su `spark.sql.shuffle.partitions`, impostato a 16). L'obiettivo è che ogni partizione Spark contenga le letture di **un solo nodo**. Nota: con un hash su poche chiavi, due nodi potrebbero teoricamente cadere nella stessa partizione (collisione); per questo `process_partition` **non assume** un nodo per partizione ma raggruppa comunque le righe per `node_id` al suo interno (vedi punto 5), restando corretto in ogni caso. Con 16 bucket per 4 nodi le collisioni sono improbabili e il parallelismo effettivo è pieno.
2. **`foreachPartition(process_partition)`** — Spark esegue `process_partition` su ogni executor, in parallelo. Nodo_1 viene elaborato su un worker, nodo_2 su un altro, e così via.
3. **Ordinamento intra-partizione** — Welford è seriale (lo z-score della lettura *N* dipende dalle letture *1…N-1*). Spark non garantisce l'ordine dopo il `filter`/`repartition`, quindi `process_partition` **riordina le righe per `reading_index`** prima di applicare l'algoritmo. Questo preserva la correttezza statistica.
4. **Connessioni per executor** — Mongo ed ES vengono aperti *dentro* la funzione di partizione (singleton per processo executor, non riutilizzabili dal driver).
5. **Robustezza** — `process_partition` raggruppa comunque per `node_id` al suo interno: se per collisione di hash due nodi finissero nella stessa partizione, il risultato resterebbe corretto.

Lo stato Welford resta seriale *all'interno* della partizione (corretto, una partizione = un nodo) ma i nodi sono elaborati **in parallelo su worker diversi**. La persistenza dello stato su MongoDB `node_stats` usa upsert atomici per `node_id`, sicuri anche con scritture concorrenti da executor diversi.

### 9.3 Una conseguenza di progetto: il node_status_index

Distribuire l'arricchimento ha un effetto sul `node_status_index`. I valori cumulativi `running_min/max` derivano dall'aggregazione globale del batch (passo 4, sul driver), ma il documento di stato per nodo viene scritto nel passo 3 (sui worker). Per non reintrodurre un `collect()`, ogni worker **legge da `agg_per_nodo` i running stat aggiornati dal batch precedente**. Ne consegue un ritardo di un micro-batch (≈5 s) sui soli min/max storici mostrati nelle card di stato — irrilevante per un cruscotto live, e un trade-off esplicito a favore della piena distribuzione.

### 9.4 Ottimizzazioni di esecuzione

- **`cache()` sul batch filtrato** — `batch_clean` è usato due volte (passo 3 e passo 4). Senza cache Spark rieseguirebbe parsing + filtro per ogni azione; con `cache()` il batch è materializzato una sola volta e liberato con `unpersist()` a fine elaborazione.
- **`groupBy` nativo invece di ricalcolo** — le statistiche di batch (min/max/avg/sum per nodo) sono calcolate una sola volta con l'API DataFrame distribuita, non in Python.

### 9.5 Limiti residui e ulteriori passi di scalabilità

L'architettura attuale distribuisce il calcolo fino a **N worker = N nodi**: con 4 nodi e 3 worker il parallelismo è pieno. Per scalare ulteriormente:

- **`flatMapGroupsWithState`** — è il pattern *canonico* di Structured Streaming per lo stato per chiave. Spark manterrebbe lo stato Welford di ogni `node_id` nel proprio **state store** con checkpoint e fault tolerance integrati, rendendo superflua la collezione `node_stats` su MongoDB. È il passo successivo naturale per un numero molto grande di sensori (migliaia di chiavi), dove gestire lo stato su Mongo diventerebbe il collo di bottiglia.
- **Kafka**: scala aumentando le partizioni del topic. Il modello *1 nodo = 1 partizione* generalizza a *N sensori = N partizioni*, con consumo parallelo proporzionale.
- **Spark**: scala orizzontalmente aggiungendo worker; il parallelismo è limitato dal numero di chiavi distinte (nodi), non più dal driver.
- **Elasticsearch / MongoDB**: oggi single-node per i vincoli di RAM del laptop; in produzione diventerebbero cluster con sharding (per `node_id`) e repliche, senza modifiche al codice della pipeline — gli endpoint sono già esternalizzati in variabili d'ambiente.

### 9.6 Sintesi

La pipeline usa il cluster Spark in modo coerente con i principi Big Data: ingestione, parsing, filtro e aggregazioni sfruttano le trasformazioni native distribuite, mentre la logica stateful per-nodo è distribuita sugli executor via `foreachPartition` con ripartizionamento per `node_id`. Non c'è `collect()` dei dati nel path di arricchimento, quindi nessun collo di bottiglia né single point of failure di memoria sul driver. Il percorso verso volumi ancora maggiori (`flatMapGroupsWithState` con state store nativo) è identificato e non richiederebbe un cambio di framework.
