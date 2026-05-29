import csv
import json
import os
import time
from datetime import datetime, timezone
from itertools import zip_longest

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC     = os.environ["KAFKA_TOPIC"]
DELAY_MS  = int(os.environ.get("PRODUCER_DELAY_MS", "500"))
LOOP      = os.environ.get("LOOP_FOREVER", "true").lower() == "true"

# Ogni nodo usa il proprio CSV reale:
#   nodo_1 → misto 44% normal / 56% fire=1  (zona di transizione)
#   nodo_2 → 100% fire=1 (prima acquisizione con accendino)
#   nodo_3 → 25% normal / 25% fire=1 / 50% fire=2 (zona ad alto rischio)
#   nodo_4 → no Fire (sensore ambientale puro, sempre verde)
CSV_MAP = {
    "nodo_1": "/data/acquisizioni/Nodo_1/prima_acquisizione/nodo1_csv.csv",
    "nodo_2": "/data/acquisizioni/Nodo_2/prima_acquisizione/nodo2.csv",
    "nodo_3": "/data/acquisizioni/Nodo_3/prima_acq/nodo3_csv.csv",
    "nodo_4": "/data/acquisizioni/Nodo_4/nodo4_csv.csv",
}

# Campi numerici da castare a float (None se stringa vuota o non numerica)
NUMERIC_FIELDS = [
    "Temperature (C)", "Humidity (%)", "Pressure (hPA)", "Gas (Ohm)",
    "Visible Light", "IR", "UV index", "CO", "NO2", "Smoke (ppm)",
]


def get_producer(retries: int = 20, wait: int = 5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                retry_backoff_ms=500,
                request_timeout_ms=120000,
                max_block_ms=30000,
            )
        except NoBrokersAvailable:
            print(f"Kafka non disponibile (tentativo {attempt + 1}/{retries}), riprovo in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossibile connettersi a Kafka dopo tutti i tentativi")


def load_csv_rows(node_id: str, path: str) -> list[dict]:
    """Legge il CSV e restituisce lista di dict normalizzati."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            record = {}
            for field in NUMERIC_FIELDS:
                raw = row.get(field, "").strip()
                if not raw:
                    record[field] = None
                else:
                    try:
                        record[field] = float(raw)
                    except ValueError:
                        record[field] = None

            # Fire: presente in nodo_1/3, None per nodo_2 (usa nodo3_normal) e nodo_4
            fire_raw = row.get("Fire", "").strip()
            try:
                record["Fire"] = int(fire_raw) if fire_raw != "" else None
            except ValueError:
                record["Fire"] = None

            record["node_id"]       = node_id
            record["reading_index"] = idx
            rows.append(record)

    print(f"  {node_id}: {len(rows)} righe caricate da {path.split('/')[-1]}")
    return rows


def _on_send_error(exc):
    print(f"[ERROR] Invio Kafka fallito: {exc}")


def main():
    producer = get_producer()
    print(f"Connesso a Kafka ({BOOTSTRAP}), topic: {TOPIC}")

    # Carica tutti i CSV in memoria — LOOP_FOREVER=true per simulazione continua
    all_rows = {node_id: load_csv_rows(node_id, path) for node_id, path in CSV_MAP.items()}
    total_per_pass = sum(len(v) for v in all_rows.values())
    print(f"Caricati {total_per_pass} record totali (4 nodi). Loop continuo: {LOOP}")

    # Stampa distribuzione Fire per conferma visiva
    for nid, rows in all_rows.items():
        fire_counts: dict = {}
        for r in rows:
            k = str(r.get("Fire"))
            fire_counts[k] = fire_counts.get(k, 0) + 1
        print(f"  {nid} Fire distribution: {dict(sorted(fire_counts.items()))}")

    delay_s = DELAY_MS / 1000.0
    sent    = 0
    pass_n  = 0

    try:
        while True:
            pass_n += 1
            print(f"--- Pass #{pass_n} ---")
            # Emissione round-robin: tutti i nodi avanzano insieme, simula sensori concorrenti.
            # zip_longest gestisce file di lunghezze diverse: i nodi esauriti vengono saltati
            # nella passata corrente, poi ripartono al prossimo giro (LOOP=true).
            iterators = {nid: iter(rows) for nid, rows in all_rows.items()}
            for batch in zip_longest(*iterators.values()):
                for node_id, record in zip(iterators.keys(), batch):
                    if record is None:
                        continue
                    # Shallow copy: non mutare il dict persistente in all_rows
                    msg = {
                        **record,
                        "ingest_ts": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%S.%f"
                        )[:-3] + "Z",
                    }
                    producer.send(TOPIC, key=node_id, value=msg).add_errback(_on_send_error)
                    sent += 1
                    if sent % 500 == 0:
                        print(f"  Inviati {sent} messaggi (pass #{pass_n})...")
                time.sleep(delay_s)

            producer.flush()
            print(f"Pass #{pass_n} completato: {sent} messaggi totali inviati.")

            if not LOOP:
                break
            # In modalità loop continuo il dato riparte dall'inizio simulando
            # un flusso IoT infinito — ideale per la dashboard live
    finally:
        producer.flush()
        producer.close()
        print("Producer terminato.")


if __name__ == "__main__":
    main()
