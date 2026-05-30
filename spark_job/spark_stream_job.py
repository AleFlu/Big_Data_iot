import os
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configurazione da variabili d'ambiente ────────────────────────────────────
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "iot.sensor.data")
MONGO_URI        = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/sensor_data")
ES_HOST          = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT          = int(os.environ.get("ES_PORT", "9200"))
ES_INDEX         = os.environ.get("ES_INDEX", "sensors_live_index")
ES_STATUS_INDEX  = "node_status_index"

# Soglie di pulizia identiche al notebook (Cell 19)
CO_MAX  = 1000
GAS_MAX = 1_000_000

# Soglie assolute per anomaly detection (dai dati reali dei CSV):
# nodo_3 fire=1/2 raggiunge CO fino a 993 ppm; baseline normale ~1-5 ppm.
# Smoke baseline ~0.01-0.04; durante eventi fire nodo_2 arriva a 0.18.
# Temperatura: range normale 20-35°C, sopra 40°C segnale di calore anomalo.
CO_ANOMALY_THRESHOLD    = 50.0   # ppm — >50 indica combustione (baseline <10)
SMOKE_ANOMALY_THRESHOLD = 0.08   # ppm — >0.08 è sopra il doppio del max baseline
TEMP_ANOMALY_THRESHOLD  = 35.0   # °C  — >35 allineato alla soglia orange Grafana

# Campi usati per z-score (identici al notebook)
ANOMALY_SENSORS = [
    ("Temperature (C)", "Temperature"),
    ("CO",              "CO"),
    ("Smoke (ppm)",     "Smoke"),
    ("Gas (Ohm)",       "Gas"),
]

# Rinomina campi per ES (snake_case, compatibili con Kibana KQL / Lens).
# I nomi originali restano invariati in MongoDB raw_readings e nei messaggi Kafka.
FIELD_RENAME_FOR_ES = {
    "Temperature (C)": "temperature_c",
    "Humidity (%)":    "humidity_pct",
    "Pressure (hPA)":  "pressure_hpa",
    "Gas (Ohm)":       "gas_ohm",
    "Visible Light":   "visible_light",
    "IR":              "ir",
    "UV index":        "uv_index",
    "CO":              "co",
    "NO2":             "no2",
    "Smoke (ppm)":     "smoke_ppm",
    "Fire":            "fire",
    "node_id":         "node_id",
    "reading_index":   "reading_index",
    "ingest_ts":       "ingest_ts",
}

# ── Schema Spark (replica esatta delle colonne CSV + campi aggiunti dal producer)
SENSOR_SCHEMA = StructType([
    StructField("Temperature (C)", DoubleType(),  True),
    StructField("Humidity (%)",    DoubleType(),  True),
    StructField("Pressure (hPA)",  DoubleType(),  True),
    StructField("Gas (Ohm)",       DoubleType(),  True),
    StructField("Visible Light",   DoubleType(),  True),
    StructField("IR",              DoubleType(),  True),
    StructField("UV index",        DoubleType(),  True),
    StructField("CO",              DoubleType(),  True),
    StructField("NO2",             DoubleType(),  True),
    StructField("Smoke (ppm)",     DoubleType(),  True),
    StructField("Fire",            IntegerType(), True),   # null per nodo_4
    StructField("node_id",         StringType(),  False),
    StructField("reading_index",   IntegerType(), True),
    StructField("ingest_ts",       StringType(),  True),   # ISO8601 UTC dal producer
])

# ── Module-level singletons ───────────────────────────────────────────────────
_es_client    = None
_mongo_client = None


def get_es_client() -> Elasticsearch:
    """Singleton ES client; crea entrambi gli indici al primo accesso."""
    global _es_client
    if _es_client is None:
        _es_client = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
        ensure_es_index(_es_client)
        ensure_node_status_index(_es_client)
    return _es_client


def get_mongo_db():
    """Singleton MongoClient; restituisce il database sensor_data."""
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI)
    return _mongo_client["sensor_data"]


def _safe_create_index(es_client: Elasticsearch, index: str, body: dict) -> None:
    """Crea un indice ES; ignora l'errore se esiste già (TOCTOU-safe)."""
    try:
        es_client.indices.create(index=index, body=body)
        print(f"Indice ES '{index}' creato.")
    except RequestError as e:
        if e.error != "resource_already_exists_exception":
            raise


def ensure_es_index(es_client: Elasticsearch) -> None:
    """Crea sensors_live_index con mapping esplicito."""
    if es_client.indices.exists(index=ES_INDEX):
        return
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "1s"}},
        "mappings": {
            "properties": {
                "node_id":            {"type": "keyword"},
                "reading_index":      {"type": "integer"},
                "ingest_ts":          {"type": "date", "format": "strict_date_optional_time"},
                "temperature_c":      {"type": "float"},
                "humidity_pct":       {"type": "float"},
                "pressure_hpa":       {"type": "float"},
                "gas_ohm":            {"type": "float"},
                "visible_light":      {"type": "float"},
                "ir":                 {"type": "float"},
                "uv_index":           {"type": "float"},
                "co":                 {"type": "float"},
                "no2":                {"type": "float"},
                "smoke_ppm":          {"type": "float"},
                "fire":               {"type": "integer"},
                "is_fire":            {"type": "boolean"},
                "is_fire_transition": {"type": "boolean"},
                "fire_state_label":   {"type": "keyword"},
                "zscore_Temperature": {"type": "float"},
                "zscore_CO":          {"type": "float"},
                "zscore_Smoke":       {"type": "float"},
                "zscore_Gas":         {"type": "float"},
                "is_anomaly":         {"type": "boolean"},
                "anomaly_sensors":    {"type": "keyword"},
            }
        },
    }
    _safe_create_index(es_client, ES_INDEX, mapping)


def ensure_node_status_index(es_client: Elasticsearch) -> None:
    """Crea node_status_index — un documento per nodo, aggiornato ogni batch."""
    if es_client.indices.exists(index=ES_STATUS_INDEX):
        return
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "5s"}},
        "mappings": {
            "properties": {
                "node_id":               {"type": "keyword"},
                "last_ingest_ts":        {"type": "date", "format": "strict_date_optional_time"},
                "temperature_c":         {"type": "float"},
                "humidity_pct":          {"type": "float"},
                "pressure_hpa":          {"type": "float"},
                "gas_ohm":               {"type": "float"},
                "co":                    {"type": "float"},
                "smoke_ppm":             {"type": "float"},
                "fire":                  {"type": "integer"},
                "is_fire":               {"type": "boolean"},
                "fire_state_label":      {"type": "keyword"},
                "is_anomaly_current":    {"type": "boolean"},
                "anomaly_sensors":       {"type": "keyword"},
                "zscore_Temperature":    {"type": "float"},
                "zscore_CO":             {"type": "float"},
                "zscore_Smoke":          {"type": "float"},
                "zscore_Gas":            {"type": "float"},
                "last_update_ts":        {"type": "date", "format": "strict_date_optional_time"},
            }
        },
    }
    _safe_create_index(es_client, ES_STATUS_INDEX, mapping)


def _fire_state_label(fire_val) -> str:
    """Mappa il valore Fire intero a una label leggibile."""
    if fire_val is None:
        return "N/A"
    if fire_val == 0:
        return "NORMAL"
    if fire_val == 1:
        return "FIRE"
    return "SPECIAL"     # fire_val == 2 (nodo_3)


def welford_update(stats: dict, short: str, val: float) -> tuple[float, float, float]:
    """
    Aggiorna le statistiche online (algoritmo Welford).
    Usa varianza campionaria (m2/(n-1)) per z-score statisticamente corretto.
    """
    n    = stats.get(f"{short}_count", 0) + 1
    mean = stats.get(f"{short}_mean",  0.0)
    m2   = stats.get(f"{short}_m2",    0.0)
    delta  = val - mean
    mean   = mean + delta / n
    delta2 = val - mean
    m2     = m2 + delta * delta2
    return n, mean, m2


def process_batch(batch_df, batch_id: int) -> None:
    """
    Callback foreachBatch — pipeline completa:
      1. Scrivi raw su MongoDB raw_readings
      2. Filtra outlier (CO > 1000 o Gas > 1M)
      3. Z-score Welford per 4 sensori
      4. Arricchimento: is_fire, fire_state_label
      5. Scrivi su MongoDB processed_readings (batch layer — dati arricchiti)
      6. Scrivi su ES sensors_live_index (serving layer — time-series)
      7. Upsert su ES node_status_index (stato corrente per nodo)
      8. Scrivi fire_events su MongoDB (solo transizioni no-fire → fire)
      9. Rolling stats su MongoDB agg_per_nodo
    """
    if batch_df.isEmpty():
        return

    # ── 1. Raw write su MongoDB ───────────────────────────────────────────────
    (batch_df.write
     .format("mongodb")
     .option("connection.uri", MONGO_URI)
     .option("collection", "raw_readings")
     .option("idFieldList", "node_id,reading_index")
     .option("operationType", "replace")
     .mode("append")
     .save())

    # ── 2. Filtro outlier ─────────────────────────────────────────────────────
    batch_clean = batch_df.filter(
        (F.col("CO") < CO_MAX) &
        (F.col("Gas (Ohm)") < GAS_MAX)
    )
    rows = batch_clean.collect()
    if not rows:
        return

    # ── 3. Z-score Welford ────────────────────────────────────────────────────
    db = get_mongo_db()

    node_ids_in_batch = list({row["node_id"] for row in rows})
    node_stats_map = {
        s["node_id"]: s
        for s in db.node_stats.find({"node_id": {"$in": node_ids_in_batch}})
    }
    for nid in node_ids_in_batch:
        if nid not in node_stats_map:
            node_stats_map[nid] = {"node_id": nid}

    enriched_rows = []
    for row in rows:
        node     = row["node_id"]
        stats    = node_stats_map[node]
        row_dict = row.asDict()

        for col_name, short in ANOMALY_SENSORS:
            val = row_dict.get(col_name)
            if val is None:
                row_dict[f"zscore_{short}"] = None
                continue
            n, mean, m2 = welford_update(stats, short, val)
            stats[f"{short}_count"] = n
            stats[f"{short}_mean"]  = mean
            stats[f"{short}_m2"]    = m2
            std    = (m2 / (n - 1)) ** 0.5 if n > 1 else 0.0
            zscore = round(abs(val - mean) / std, 3) if std > 0 else 0.0
            row_dict[f"zscore_{short}"] = zscore

        anomaly_flags = [
            short for _, short in ANOMALY_SENSORS
            if (row_dict.get(f"zscore_{short}") or 0.0) > 2.0
        ]
        # Soglie assolute: scattano indipendentemente dallo z-score.
        # Utili nelle prime letture (Welford ha poca storia) e quando
        # tutti i valori sono alti (z-score basso ma valore pericoloso).
        # Usano gli stessi nomi dei flag z-score per coerenza nei filtri Grafana.
        co_val    = row_dict.get("CO")
        smoke_val = row_dict.get("Smoke (ppm)")
        temp_val  = row_dict.get("Temperature (C)")
        if co_val    is not None and co_val    > CO_ANOMALY_THRESHOLD    and "CO"          not in anomaly_flags:
            anomaly_flags.append("CO")
        if smoke_val is not None and smoke_val > SMOKE_ANOMALY_THRESHOLD and "Smoke"       not in anomaly_flags:
            anomaly_flags.append("Smoke")
        if temp_val  is not None and temp_val  > TEMP_ANOMALY_THRESHOLD  and "Temperature" not in anomaly_flags:
            anomaly_flags.append("Temperature")

        row_dict["is_anomaly"]      = len(anomaly_flags) > 0
        row_dict["anomaly_sensors"] = ", ".join(anomaly_flags)

        # ── 4. Arricchimento fire ─────────────────────────────────────────────
        fire_val  = row_dict.get("Fire")
        last_fire = stats.get("last_fire_value")   # None = nodo mai visto prima
        # Transizione: nodo passa da no-fire (None o 0) a fire (>=1)
        is_transition = bool(
            fire_val is not None
            and fire_val >= 1
            and (last_fire is None or last_fire == 0)
        )
        row_dict["is_fire"]            = bool(fire_val is not None and fire_val >= 1)
        row_dict["is_fire_transition"] = is_transition
        row_dict["fire_value_prev"]    = int(last_fire) if last_fire is not None else 0
        row_dict["fire_state_label"]   = _fire_state_label(fire_val)
        # Aggiorna last_fire_value per le righe successive dello stesso nodo nel batch
        if fire_val is not None:
            stats["last_fire_value"] = fire_val

        enriched_rows.append(row_dict)

    # Bulk write statistiche Welford aggiornate
    for nid, stats in node_stats_map.items():
        stats.pop("_id", None)
    db.node_stats.bulk_write([
        ReplaceOne({"node_id": nid}, stats, upsert=True)
        for nid, stats in node_stats_map.items()
    ])

    # ── 5. Scrivi su MongoDB processed_readings (dati arricchiti, layer batch) ──
    # Source of truth per rianalisi future: contiene i dati post-filtro con
    # z-score, flag anomalia e fire_state_label — non ricalcolabili da raw_readings
    # senza rieseguire Welford dall'inizio. Separazione Lambda: raw = immutabile,
    # processed = elaborato, ES = serving layer per query veloci.
    processed_docs = []
    for row_dict in enriched_rows:
        doc = {k: v for k, v in row_dict.items()
               if not (isinstance(v, float) and v != v)}  # scarta NaN
        if doc.get("Fire") is not None:
            doc["Fire"] = int(doc["Fire"])
        doc["is_anomaly"] = bool(doc.get("is_anomaly", False))
        doc["is_fire"]    = bool(doc.get("is_fire", False))
        processed_docs.append(doc)

    if processed_docs:
        db.processed_readings.bulk_write([
            ReplaceOne(
                {"node_id": d["node_id"], "reading_index": d["reading_index"]},
                d, upsert=True
            )
            for d in processed_docs
        ], ordered=False)

    # ── 6. Scrivi su ES sensors_live_index (serving layer) ───────────────────
    es = get_es_client()
    actions = []
    for row_dict in enriched_rows:
        doc = {}
        for k, v in row_dict.items():
            if isinstance(v, float) and v != v:   # scarta NaN
                continue
            doc[FIELD_RENAME_FOR_ES.get(k, k)] = v
        if "fire" in doc and doc["fire"] is not None:
            doc["fire"] = int(doc["fire"])
        doc["is_anomaly"] = bool(doc.get("is_anomaly", False))
        doc["is_fire"]    = bool(doc.get("is_fire", False))
        actions.append({
            "_index": ES_INDEX,
            "_id":    f"{doc.get('node_id', '')}_{doc.get('reading_index', '')}",
            "_source": doc,
        })

    if actions:
        success, errors = helpers.bulk(
            es, actions, chunk_size=500, raise_on_error=False, raise_on_exception=True
        )
        if errors:
            print(f"[WARN] sensors_live bulk: {len(errors)} failed in batch {batch_id}")

    # ── 7. Upsert su ES node_status_index (1 doc per nodo) ───────────────────
    # Per ogni nodo presente nel batch, prendi l'ultima riga (max ingest_ts)
    # e upsertala come documento di stato corrente.
    latest_per_node: dict[str, dict] = {}
    for row_dict in enriched_rows:
        nid = row_dict["node_id"]
        if nid not in latest_per_node or row_dict["ingest_ts"] > latest_per_node[nid]["ingest_ts"]:
            latest_per_node[nid] = row_dict

    status_actions = []
    for nid, rd in latest_per_node.items():
        fire_val = rd.get("Fire")
        status_doc = {
            "node_id":            nid,
            "last_ingest_ts":     rd.get("ingest_ts"),
            "temperature_c":      rd.get("Temperature (C)"),
            "humidity_pct":       rd.get("Humidity (%)"),
            "pressure_hpa":       rd.get("Pressure (hPA)"),
            "gas_ohm":            rd.get("Gas (Ohm)"),
            "co":                 rd.get("CO"),
            "smoke_ppm":          rd.get("Smoke (ppm)"),
            "fire":               int(fire_val) if fire_val is not None else None,
            "is_fire":            bool(fire_val is not None and fire_val >= 1),
            "fire_state_label":   _fire_state_label(fire_val),
            "is_anomaly_current": bool(rd.get("is_anomaly", False)),
            "anomaly_sensors":    rd.get("anomaly_sensors", ""),
            "zscore_Temperature": rd.get("zscore_Temperature"),
            "zscore_CO":          rd.get("zscore_CO"),
            "zscore_Smoke":       rd.get("zscore_Smoke"),
            "zscore_Gas":         rd.get("zscore_Gas"),
            "last_update_ts":     datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3] + "Z",
        }
        # Upsert: usa node_id come _id così c'è sempre 1 documento per nodo
        status_actions.append({
            "_index":  ES_STATUS_INDEX,
            "_id":     nid,
            "_source": status_doc,
        })

    if status_actions:
        success, errors = helpers.bulk(
            es, status_actions, chunk_size=10, raise_on_error=False, raise_on_exception=True
        )
        if errors:
            print(f"[WARN] node_status bulk: {len(errors)} failed in batch {batch_id}")

    # ── 8. Fire events su MongoDB (solo transizioni no-fire → fire) ──────────
    fire_rows = [r for r in enriched_rows if r.get("is_fire_transition")]
    if fire_rows:
        fire_docs = []
        for rd in fire_rows:
            fire_docs.append({
                "node_id":        rd["node_id"],
                "reading_index":  rd.get("reading_index"),
                "fire_value":     int(rd["Fire"]),
                "fire_value_prev": rd.get("fire_value_prev", 0),
                "ingest_ts":      datetime.fromisoformat(rd["ingest_ts"].replace("Z", "+00:00")),
                "temperature_c":  rd.get("Temperature (C)"),
                "co":             rd.get("CO"),
                "smoke_ppm":      rd.get("Smoke (ppm)"),
            })
        db.fire_events.bulk_write([
            ReplaceOne(
                {"node_id": d["node_id"], "reading_index": d.get("reading_index")},
                d, upsert=True
            )
            for d in fire_docs
        ], ordered=False)

    # ── 9. Rolling stats su MongoDB agg_per_nodo ──────────────────────────────
    agg_rows = (batch_clean
                .groupBy("node_id")
                .agg(
                    F.count("*").alias("batch_count"),
                    F.round(F.avg(F.col("Temperature (C)")),  2).alias("batch_avg_temp"),
                    F.round(F.min(F.col("Temperature (C)")),  2).alias("batch_min_temp"),
                    F.round(F.max(F.col("Temperature (C)")),  2).alias("batch_max_temp"),
                    F.round(F.sum(F.col("Temperature (C)")),  4).alias("batch_sum_temp"),
                    F.round(F.avg(F.col("CO")),                2).alias("batch_avg_co"),
                    F.round(F.min(F.col("CO")),                2).alias("batch_min_co"),
                    F.round(F.max(F.col("CO")),                2).alias("batch_max_co"),
                    F.round(F.sum(F.col("CO")),                4).alias("batch_sum_co"),
                    F.round(F.avg(F.col("Smoke (ppm)")),       4).alias("batch_avg_smoke"),
                    F.round(F.min(F.col("Smoke (ppm)")),       4).alias("batch_min_smoke"),
                    F.round(F.max(F.col("Smoke (ppm)")),       4).alias("batch_max_smoke"),
                    F.round(F.sum(F.col("Smoke (ppm)")),       4).alias("batch_sum_smoke"),
                    F.round(F.avg(F.col("Gas (Ohm)")),         2).alias("batch_avg_gas"),
                    F.round(F.min(F.col("Gas (Ohm)")),         2).alias("batch_min_gas"),
                    F.round(F.max(F.col("Gas (Ohm)")),         2).alias("batch_max_gas"),
                    F.round(F.sum(F.col("Gas (Ohm)")),         2).alias("batch_sum_gas"),
                )
                .collect())

    for agg in agg_rows:
        db.agg_per_nodo.update_one(
            {"node_id": agg["node_id"]},
            {
                # Contatori e somme cumulativi (atomic)
                "$inc": {
                    "total_processed":  agg["batch_count"],
                    "running_sum_temp": float(agg["batch_sum_temp"] or 0),
                    "running_sum_co":   float(agg["batch_sum_co"]   or 0),
                    "running_sum_smoke":float(agg["batch_sum_smoke"] or 0),
                    "running_sum_gas":  float(agg["batch_sum_gas"]   or 0),
                },
                # Min/max cumulativi — $min/$max sono atomici su MongoDB 7.0
                # Guard None: $min/$max con null resetta il valore su MongoDB 7.0
                "$min": {k: v for k, v in {
                    "running_min_temp":  agg["batch_min_temp"],
                    "running_min_co":    agg["batch_min_co"],
                    "running_min_smoke": agg["batch_min_smoke"],
                    "running_min_gas":   agg["batch_min_gas"],
                }.items() if v is not None},
                "$max": {k: v for k, v in {
                    "running_max_temp":  agg["batch_max_temp"],
                    "running_max_co":    agg["batch_max_co"],
                    "running_max_smoke": agg["batch_max_smoke"],
                    "running_max_gas":   agg["batch_max_gas"],
                }.items() if v is not None},
                # Ultimi valori del batch per riferimento rapido
                "$set": {
                    "last_batch_avg_temp":  agg["batch_avg_temp"],
                    "last_batch_avg_co":    agg["batch_avg_co"],
                    "last_batch_avg_smoke": agg["batch_avg_smoke"],
                    "last_batch_avg_gas":   agg["batch_avg_gas"],
                    "last_update":          datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )

    fire_count    = sum(1 for r in enriched_rows if r.get("is_fire"))
    anomaly_count = sum(1 for r in enriched_rows if r["is_anomaly"])
    print(
        f"Batch {batch_id}: {len(enriched_rows)} record, "
        f"{fire_count} fire, {anomaly_count} anomalie → ES + MongoDB"
    )


def main():
    spark = (SparkSession.builder
             .appName("IoT-Sensor-Streaming")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    df_raw = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", KAFKA_TOPIC)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 200)
              .load())

    df_parsed = (df_raw
                 .select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
                 .select("d.*"))

    query = (df_parsed.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "/spark/checkpoints/iot_stream")
             .trigger(processingTime="5 seconds")
             .outputMode("append")
             .start())

    print(f"Streaming query avviata. Topic: {KAFKA_TOPIC} → ES:{ES_INDEX} + {ES_STATUS_INDEX} + MongoDB")
    query.awaitTermination()


if __name__ == "__main__":
    main()
