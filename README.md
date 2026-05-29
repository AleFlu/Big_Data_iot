# IoT Big Data Streaming Pipeline

Pipeline di analisi **streaming live** per una rete di 4 sensori IoT. I dati ambientali (temperatura, CO, smoke, gas, pressione, umidità) vengono trasmessi da 4 nodi indipendenti, elaborati in tempo reale da Spark con rilevamento anomalie, e visualizzati su dashboard Grafana aggiornate ogni 5 secondi.

Il sistema simula un **deployment distribuito su 4 macchine**: ogni nodo sensore ha il proprio container producer indipendente, Spark gira come cluster Standalone (1 master + 3 worker), e ogni componente comunica via hostname esplicito come farebbe su host fisici separati.

---

## Architettura

```
[macchina-1]  csv-producer-1 (nodo_1) ──┐
[macchina-2]  csv-producer-2 (nodo_2) ──┤──► Kafka (4 partizioni) ──► Spark Standalone Cluster
[macchina-3]  csv-producer-3 (nodo_3) ──┤                                 │
[macchina-4]  csv-producer-4 (nodo_4) ──┘                        ┌────────┴────────┐
                                                           MongoDB (5 collection)  Elasticsearch (2 indici)
                                                                                         │
                                                                                      Grafana
```

### Stack tecnologico

| Componente | Tecnologia | Versione |
|---|---|---|
| Message broker | Confluent Kafka (KRaft) | 7.6.1 |
| Stream processing | Apache Spark Structured Streaming | 3.5.3 |
| Time-series store | Elasticsearch | 7.17.28 |
| Document store | MongoDB | 7.0 |
| Dashboard | Grafana | 10.4.2 |
| Producer | Python + kafka-python | 3.11 |
| Platform | Docker Compose, ARM64 (Apple Silicon) | — |

---

## Struttura del progetto

```
.
├── docker-compose.yml          # Orchestrazione completa (14 servizi)
├── .env.example                # Template variabili d'ambiente
├── csv_producer/
│   ├── csv_producer.py         # Producer parametrizzato (NODE_ID + CSV_PATH)
│   ├── Dockerfile
│   └── requirements.txt
├── spark_job/
│   ├── spark_stream_job.py     # Pipeline Spark (~510 righe): Welford, ES, MongoDB
│   ├── entrypoint.sh           # Dispatch SPARK_ROLE: master | worker | driver
│   ├── submit.sh               # spark-submit verso cluster standalone
│   ├── Dockerfile
│   └── requirements.txt
├── mongo_init/
│   └── init.js                 # Crea collection e indici al primo avvio
├── grafana/
│   └── provisioning/           # Dashboard e datasource provisionati via file
├── architettura.svg
├── RELAZIONE_TECNICA.md
└── pipeline_spiegazione.pdf    # Guida al flusso dati (no codice)
```

> **Non inclusi nel repo** (da fornire manualmente):
> - `.env` — generare da `.env.example`
> - `acquisizioni/` — CSV dei sensori fisici

---

## Prerequisiti

- **Docker Desktop** in esecuzione (versione Apple Silicon su Mac M1/M2)
- **8 GB** allocati alla Docker VM (il sistema usa ~6.4 GB)
- I file CSV dei sensori nella cartella `acquisizioni/` (struttura sotto)

---

## Setup iniziale

### 1. Clonare il repo

```bash
git clone https://github.com/AleFlu/Big_Data_iot.git
cd Big_Data_iot
```

### 2. Creare il file `.env`

```bash
cp .env.example .env
```

Generare un `CLUSTER_ID` per Kafka KRaft:

```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

Incollare il valore nel `.env`:

```
CLUSTER_ID=<uuid-generato>
PRODUCER_DELAY_MS=500
ES_HEAP_SIZE=512m
MONGO_INITDB_DATABASE=sensor_data
```

### 3. Preparare i CSV dei sensori

Creare la struttura `acquisizioni/` con i file reali dei sensori:

```
acquisizioni/
├── Nodo_1/prima_acquisizione/nodo1_csv.csv   # 1.733 righe, con colonna Fire
├── Nodo_2/prima_acquisizione/nodo2.csv       # 1.732 righe, con colonna Fire
├── Nodo_3/prima_acq/nodo3_csv.csv            # 1.669 righe, con colonna Fire
└── Nodo_4/nodo4_csv.csv                      # 1.611 righe, senza colonna Fire
```

I CSV hanno 10-11 colonne: `Temperature (C)`, `Humidity (%)`, `Pressure (hPA)`, `Gas (Ohm)`, `Visible Light`, `IR`, `UV index`, `CO`, `NO2`, `Smoke (ppm)`, `Fire` (opzionale).

---

## Avvio

```bash
# 1. Build immagini custom
docker compose build

# 2. Infrastruttura core
docker compose up -d kafka mongodb elasticsearch

# 3. Crea topic Kafka (aspetta ~30-60s che kafka sia healthy)
docker compose up kafka-init

# 4. Spark cluster (master prima dei worker)
docker compose up -d spark-master
# Verifica: http://localhost:8082 → Spark Master UI, 0 Workers
docker compose up -d spark-worker-1 spark-worker-2 spark-worker-3
# Verifica: http://localhost:8082 → 3 Workers, 3 Cores Total

# 5. Spark job (submit al cluster)
docker compose up -d spark-job
docker compose logs -f spark-job
# Aspetta: "Streaming query avviata"

# 6. CSV Producer x4 (partono a inviare dati)
docker compose up -d csv-producer-1 csv-producer-2 csv-producer-3 csv-producer-4

# 7. Tool di monitoring
docker compose up -d grafana mongo-express kafka-ui
```

### Avvio completo in un comando (dopo il primo setup)

```bash
docker compose up -d
```

---

## Servizi e porte

| Servizio | URL | Credenziali |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Spark Master UI | http://localhost:8082 | — |
| Kafka UI | http://localhost:8080 | — |
| Mongo Express | http://localhost:8081 | — |
| Elasticsearch | http://localhost:9200 | — |
| MongoDB | localhost:27017 | — |
| Kafka (host) | localhost:9092 | — |

---

## Pipeline dati

```
CSV Producer x4
    │  JSON via Kafka (key=node_id, partizione esplicita 0-3)
    ▼
Kafka topic: iot.sensor.data (4 partizioni, retention 24h)
    │  micro-batch ogni 5s, max 200 offset/partizione
    ▼
Spark Structured Streaming — foreachBatch
    │
    ├─► MongoDB raw_readings        ← copia grezzo pre-filtro (storico immutabile)
    │
    │   [filtro outlier: CO>1000 o Gas>1M scartati]
    │   [z-score Welford per: Temperature, CO, Smoke, Gas]
    │   [soglie assolute: CO>50, Smoke>0.08, Temp>35°C]
    │   [flag: is_anomaly, is_fire, fire_state_label]
    │
    ├─► MongoDB processed_readings  ← dati arricchiti (batch layer, Lambda Architecture)
    ├─► Elasticsearch sensors_live_index  ← serie temporale per Grafana
    ├─► Elasticsearch node_status_index   ← stato corrente (1 doc per nodo)
    ├─► MongoDB fire_events         ← solo eventi Fire >= 1
    └─► MongoDB agg_per_nodo        ← statistiche cumulative rolling
```

### Anomaly detection

Il sistema usa due meccanismi complementari:

- **Welford online** (z-score): media e varianza calcolate in modo incrementale, senza tenere in memoria tutta la storia. Stato `{count, mean, m2}` persistito su MongoDB `node_stats` per sopravvivere ai riavvii. Anomalia se `z > 2σ`.
- **Soglie assolute**: CO > 50 ppm, Smoke > 0.08 ppm, Temperatura > 35°C. Coprono il periodo di warm-up di Welford e i nodi con baseline strutturalmente alta.

### Lambda Architecture

| Layer | Collection/Indice | Contenuto |
|---|---|---|
| Batch (immutabile) | `raw_readings` | Ogni messaggio grezzo pre-filtro |
| Batch (elaborato) | `processed_readings` | Post-filtro con z-score e flag anomalia |
| Serving (real-time) | `sensors_live_index` | Serie temporale per Grafana |
| Serving (stato) | `node_status_index` | Istantanea corrente per nodo |

---

## Spark Standalone Cluster

La stessa immagine Docker copre tutti i ruoli Spark. Il ruolo è selezionato da `SPARK_ROLE`:

| Container | SPARK_ROLE | Funzione |
|---|---|---|
| spark-master | master | Coordinatore — accetta job, distribuisce task |
| spark-worker-1/2/3 | worker | Executor — elaborano i task (1 core, 512m ciascuno) |
| spark-job | driver | Invia il job al master, scrive su ES e MongoDB |

Il driver gira in **client mode**: la logica Python (`foreachBatch`) gira nel container `spark-job`, non nei worker. I worker eseguono solo i task di parsing e filtering assegnati dal master.

---

## Reset completo

```bash
# Ferma tutto e cancella i volumi (dati ES, MongoDB, Kafka, checkpoint Spark)
docker compose down -v

# Rebuild da zero
docker compose build --no-cache
```

---

## Memoria Docker consigliata

| Servizio | mem_limit |
|---|---|
| Elasticsearch | 1024 MB |
| Spark worker x3 | 640 MB × 3 |
| Spark driver | 768 MB |
| Spark master | 512 MB |
| Kafka | 512 MB |
| MongoDB | 512 MB |
| CSV producer x4 | 128 MB × 4 |
| Grafana + tools | ~640 MB |
| **Totale** | **~6.4 GB** |

Impostare almeno **8 GB** nella Docker Desktop VM (Impostazioni → Resources → Memory).
