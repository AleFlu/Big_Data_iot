const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, TableOfContents, PageNumber, PageBreak, Footer, Header,
  VerticalAlign, ImageRun,
} = require("docx");

// ── Helpers ───────────────────────────────────────────────────────────────────
const R = (text, opts = {}) => new TextRun({ text, font: opts.mono ? "Consolas" : undefined, ...opts });

const h1 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(text)] });
const h2 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(text)] });

const p = (text) => new Paragraph({ spacing: { after: 120 }, children: [new TextRun(text)] });
const pRuns = (runs) => new Paragraph({ spacing: { after: 120 }, children: runs });
const pItalic = (text) => new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text, italics: true })] });

const bullet = (text) => new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60 }, children: [new TextRun(text)] });
const bulletRuns = (runs) => new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60 }, children: runs });
const numItem = (runs) => new Paragraph({ numbering: { reference: "numbers", level: 0 }, spacing: { after: 60 }, children: runs });

// Monospace block (one Paragraph per line)
const mono = (linesStr) => linesStr.split("\n").map((line) =>
  new Paragraph({
    spacing: { after: 0, line: 240 },
    shading: { fill: "F3F4F6", type: ShadingType.CLEAR },
    children: [new TextRun({ text: line || " ", font: "Consolas", size: 16 })],
  })
);

// Callout (nota per il docente)
const callout = (runs) => new Paragraph({
  spacing: { before: 120, after: 200 },
  shading: { fill: "FFF8E1", type: ShadingType.CLEAR },
  border: { left: { style: BorderStyle.SINGLE, size: 18, color: "F0A500", space: 8 } },
  indent: { left: 200 },
  children: runs,
});

// ── Tables ──────────────────────────────────────────────────────────────────
const CW = 9026; // content width A4 con margini 1"
const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const cellBorders = { top: border, bottom: border, left: border, right: border };
const HEAD_FILL = "D5E8F0";

function makeTable(colWidths, header, rows) {
  const headerRow = new TableRow({
    tableHeader: true,
    children: header.map((txt, i) => new TableCell({
      borders: cellBorders,
      width: { size: colWidths[i], type: WidthType.DXA },
      shading: { fill: HEAD_FILL, type: ShadingType.CLEAR },
      margins: { top: 60, bottom: 60, left: 120, right: 120 },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({ children: [new TextRun({ text: txt, bold: true })] })],
    })),
  });
  const bodyRows = rows.map((cells) => new TableRow({
    children: cells.map((cell, i) => new TableCell({
      borders: cellBorders,
      width: { size: colWidths[i], type: WidthType.DXA },
      margins: { top: 60, bottom: 60, left: 120, right: 120 },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({ children: Array.isArray(cell) ? cell : [new TextRun(cell)] })],
    })),
  }));
  return new Table({
    width: { size: CW, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [headerRow, ...bodyRows],
  });
}

const blank = () => new Paragraph({ children: [new TextRun(" ")] });

// ── Frontespizio ──────────────────────────────────────────────────────────────
const center = (text, opts = {}) => new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: opts.after ?? 120 },
  children: [new TextRun({ text, ...opts })],
});

const titlePage = [
  center("UNIVERSITÀ DEGLI STUDI DI CAGLIARI", { bold: true, size: 28, after: 60 }),
  center("Corso di Laurea Magistrale", { size: 24, after: 40 }),
  center("Insegnamento di Big Data", { size: 24, after: 1200 }),
  blank(), blank(), blank(),
  center("Pipeline Big Data per il monitoraggio IoT ambientale", { bold: true, size: 36, after: 40 }),
  center("e il rilevamento di incendi", { bold: true, size: 36, after: 200 }),
  center("Bozza di relazione", { italics: true, size: 26, after: 1400 }),
  blank(), blank(), blank(),
  center("Docente", { bold: true, size: 24, after: 40 }),
  center("Prof. Diego Reforgiato Recupero", { size: 24, after: 400 }),
  center("Candidati", { bold: true, size: 24, after: 40 }),
  center("Alessandro Piras   —   Federico Basciu", { size: 24, after: 1000 }),
  blank(), blank(),
  center("Anno Accademico 2025/2026", { size: 22, after: 20 }),
  center("9 giugno 2026", { size: 22, after: 0 }),
  new Paragraph({ children: [new PageBreak()] }),
];

// ── Indice ─────────────────────────────────────────────────────────────────────
const tocBlock = [
  new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun("Indice")] }),
  new TableOfContents("Indice", { hyperlink: true, headingStyleRange: "1-3" }),
  new Paragraph({ children: [new PageBreak()] }),
];

// ── Corpo ───────────────────────────────────────────────────────────────────────
const body = [
  callout([
    R("Nota per il docente. ", { bold: true, italics: true }),
    R("Questa è una bozza preliminare: l'obiettivo è sottoporre l'impostazione del progetto (obiettivi, stack, trattamento dei dati) per capire se l'impianto è adeguato e se conviene aggiungere o togliere qualcosa prima della versione finale. In coda alla relazione sono raccolti i punti su cui si chiede conferma e alcune possibili estensioni.", { italics: true }),
  ]),

  // 1
  h1("1. Obiettivo del progetto"),
  pRuns([
    R("Realizzare una "),
    R("pipeline Big Data end-to-end, interamente containerizzata", { bold: true }),
    R(", che acquisisce in tempo reale i dati di "),
    R("4 nodi sensore IoT ambientali", { bold: true }),
    R(", li elabora in streaming con "),
    R("rilevamento di anomalie e di incendi", { bold: true }),
    R(", e li rende disponibili su dashboard live aggiornate ogni pochi secondi."),
  ]),
  p("Il progetto mette in pratica i temi del corso su un caso realistico:"),
  bullet("Ingestione in tempo reale di flussi continui da sorgenti multiple e indipendenti;"),
  bullet("Stream processing distribuito con elaborazione stateful (statistiche online);"),
  bullet("Architettura Lambda con speed layer (streaming) e batch layer (analisi storica);"),
  bullet("Persistenza poliglotta (document store + motore di ricerca/time-series);"),
  bullet("Tolleranza ai guasti a livello di message broker;"),
  bullet("Visualizzazione e alerting sui dati elaborati."),
  pRuns([
    R("Caso d'uso applicativo: "),
    R("early warning di incendi", { bold: true }),
    R(" in ambiente boschivo a partire da sensori di gas, fumo, temperatura e qualità dell'aria."),
  ]),

  // 2
  h1("2. I dati"),
  h2("2.1 Origine e natura"),
  p("I dati provengono da acquisizioni reali effettuate con nodi sensore fisici in condizioni diverse: scenari normali e scenari di combustione controllata (es. aghi di pino, pino/lentisco). Ogni nodo ha prodotto file CSV che vengono rigiocati in streaming verso la pipeline, simulando un deployment in cui ogni nodo è una macchina separata che pubblica le proprie letture."),
  pRuns([
    R("I nodi sono 4 (nodo_1 … nodo_4), per un totale di "),
    R("6 741 letture", { bold: true }),
    R(" (rispettivamente 1 732 / 1 731 / 1 668 / 1 610 righe). Ogni nodo viene rigiocato in loop continuo, quindi il flusso verso la pipeline è di fatto illimitato."),
  ]),
  h2("2.2 Struttura di una lettura"),
  p("Ogni riga/messaggio contiene le grandezze ambientali misurate:"),
  makeTable([2600, 6426], ["Campo", "Descrizione"], [
    ["Temperature (C)", "Temperatura"],
    ["Humidity (%)", "Umidità relativa"],
    ["Pressure (hPA)", "Pressione"],
    ["Gas (Ohm)", "Resistenza del sensore di gas (cala in presenza di volatili/fumo)"],
    ["Visible Light, IR, UV index", "Luce visibile, infrarosso, indice UV"],
    ["CO, NO2", "Monossido di carbonio, biossido di azoto"],
    ["Smoke (ppm)", "Fumo"],
    [[new TextRun({ text: "Fire", bold: true })], [new TextRun({ text: "Etichetta ground-truth (0 = normale, ≥1 = incendio)", bold: true })]],
  ]),
  blank(),
  pRuns([
    R("Il campo Fire funge da "),
    R("verità di riferimento", { bold: true }),
    R(" per validare il rilevamento. È presente su nodo_1/2/3 ma "),
    R("assente su nodo_4", { bold: true }),
    R(": questo nodo viene quindi usato come caso senza etichetta, su cui la detection lavora in modo non supervisionato e che viene escluso dalla validazione."),
  ]),
  h2("2.3 Arricchimenti aggiunti dalla pipeline"),
  p("In fase di pubblicazione ogni messaggio viene completato con: node_id (nodo di provenienza), reading_index (indice progressivo di lettura) e ingest_ts (timestamp ISO-8601 UTC, usato come event-time per le finestre)."),

  // 3
  h1("3. Lo stack tecnologico"),
  makeTable([3300, 4226, 1500], ["Ruolo", "Tecnologia", "Versione"], [
    ["Message broker", "Apache Kafka (cluster KRaft, 3 broker, senza ZooKeeper)", "7.6.1"],
    ["Stream processing", "Apache Spark Structured Streaming (Standalone: 1 master + 3 worker)", "3.5.3"],
    ["Document store", "MongoDB", "7.0"],
    ["Time-series / search", "Elasticsearch", "7.17"],
    ["Dashboard", "Grafana", "10.4"],
    ["Producer", "Python + kafka-python", "3.11"],
    ["Orchestrazione", "Docker Compose (14 servizi)", "—"],
  ]),
  blank(),
  pRuns([
    R("L'intero stack gira in locale tramite Docker Compose, ma è "),
    R("progettato come se fosse multi-macchina", { bold: true }),
    R(": ogni nodo sensore è un container distinto, Spark è un cluster vero (master + 3 worker), Kafka è un cluster a 3 broker, e i servizi comunicano per hostname come farebbero su host fisici separati."),
  ]),

  // 4
  h1("4. Architettura della pipeline"),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 120, after: 120 },
    children: [new ImageRun({
      data: fs.readFileSync("architettura.png"),
      transformation: { width: 440, height: 416 },
    })],
  }),
  pItalic("Figura 1 — Architettura della pipeline: dai 4 nodi sensore fino alle dashboard Grafana."),
  blank(),
  p("Flusso ad alto livello:"),
  numItem([R("4 producer Python", { bold: true }), R(" leggono i CSV dei rispettivi nodi e pubblicano le letture come messaggi JSON su Kafka. Ogni nodo scrive su una partizione dedicata (0–3): così l'ordine di lettura per nodo è garantito.")]),
  numItem([R("Kafka", { bold: true }), R(" (topic iot.sensor.data, 4 partizioni, replication-factor 3, min.insync.replicas 2) bufferizza i messaggi e garantisce la tolleranza ai guasti: il cluster sopravvive alla perdita di un broker senza perdere dati.")]),
  numItem([R("Spark Structured Streaming", { bold: true }), R(" consuma in micro-batch ogni 5 secondi, elabora e arricchisce i dati e scrive su più sink.")]),
  numItem([R("MongoDB ed Elasticsearch", { bold: true }), R(" persistono i dati grezzi, elaborati e aggregati.")]),
  numItem([R("Grafana", { bold: true }), R(" interroga Elasticsearch e mostra dashboard live (aggiornamento ogni 5 s).")]),

  // 5
  h1("5. Come tratto i dati"),
  p("Il cuore del progetto è il job Spark. Per ogni micro-batch:"),
  h2("5.1 Ingestione e storico grezzo"),
  p("Il batch viene prima scritto così com'è su MongoDB (raw_readings): è lo storico immutabile, base per qualunque rielaborazione futura (con TTL per non crescere all'infinito)."),
  h2("5.2 Pulizia (cleaning)"),
  p("Filtro degli outlier fisicamente impossibili (es. CO > 1000 ppm, Gas > 1 MΩ): valori palesemente errati del sensore, scartati prima dell'analisi."),
  h2("5.3 Arricchimento e rilevamento anomalie"),
  p("Per ogni nodo (l'elaborazione stateful è distribuita sui worker, una partizione = un nodo, via foreachPartition) si calcola:"),
  bulletRuns([R("Z-score online (algoritmo di Welford)", { bold: true }), R(" su temperatura, CO, fumo e gas. Media e varianza sono aggiornate incrementalmente, senza conservare lo storico, e lo stato {count, mean, m2} è persistito su MongoDB così da sopravvivere ai riavvii. Lo z-score è firmato (positivo per picchi, negativo per cali → utile a distinguere un'impennata da un guasto). Anomalia se |z| > 2σ.")]),
  bulletRuns([R("Soglie assolute", { bold: true }), R(" (CO > 50 ppm, fumo > 0.08, temperatura > 35 °C, gas < 5 kΩ), tarate sui dati reali. Coprono il warm-up dello z-score (quando c'è poca storia) e i casi con baseline strutturalmente alta.")]),
  pRuns([R("I due meccanismi lavorano "), R("in parallelo e in modo complementare", { bold: true }), R(".")]),
  h2("5.4 Logica di transizione “fire”"),
  pRuns([R("Un evento incendio viene registrato "), R("solo nella transizione", { bold: true }), R(" da Fire = 0 a Fire ≥ 1 (non a ogni riga in cui Fire ≥ 1): questo evita di inondare la collezione di eventi ridondanti. Il valore precedente è persistito per nodo. È già predisposto, opzionale, l'invio di un alert via webhook in modo asincrono, fuori dal percorso critico del micro-batch.")]),
  h2("5.5 Aggregazioni a finestra"),
  pRuns([R("Una seconda query streaming indipendente (proprio reader e checkpoint) calcola aggregati su "), R("finestre temporali tumbling di 1 minuto", { bold: true }), R(" con watermark di 2 minuti (media/max/min dei sensori per nodo), scritti su un indice dedicato. È l'esempio canonico di event-time windowing con gestione dei dati in ritardo.")]),
  h2("5.6 Persistenza (architettura Lambda)"),
  makeTable([2300, 3200, 3526], ["Layer", "Destinazione", "Contenuto"], [
    ["Speed — raw", "MongoDB raw_readings", "Ogni messaggio, pre-filtro"],
    ["Speed — processed", "MongoDB processed_readings", "Dati arricchiti (z-score, flag)"],
    ["Serving — time-series", "ES sensors_live_index", "Serie storiche per Grafana"],
    ["Serving — stato live", "ES node_status_index", "1 documento per nodo (stato corrente)"],
    ["Serving — finestre", "ES window_stats", "Aggregati a finestra"],
    ["Stato", "MongoDB node_stats, agg_per_nodo", "Welford + rolling stats cumulative"],
    ["Eventi", "MongoDB fire_events", "Solo transizioni di incendio"],
  ]),
  blank(),
  h2("5.7 Batch layer storico"),
  p("Un job Spark batch separato (batch_analytics.py) rilegge l'intera raw_readings e calcola, in modo on-demand, analisi pesanti che lo streaming non può fare (richiedono l'intero dataset):"),
  bullet("percentili (p50/p95/p99), baseline e trend orari per nodo;"),
  bulletRuns([R("matrice di correlazione di Pearson", { bold: true }), R(" tra i sensori (con Spark MLlib), globale e per nodo → quali grandezze “si muovono insieme” durante la combustione;")]),
  bulletRuns([R("validazione del rilevamento contro il ground-truth", { bold: true }), R(" Fire: confusion matrix e precision / recall / F1 / accuracy per nodo.")]),
  pRuns([R("Lo snapshot risultante va su MongoDB (node_baseline) con mirror su Elasticsearch per Grafana. Questo chiude l'"), R("architettura Lambda", { bold: true }), R(": speed layer (streaming a 5 s) + batch layer (analisi a freddo sull'intero storico).")]),

  // 6
  h1("6. Visualizzazione"),
  p("Le dashboard Grafana sono provisioning-as-code (definite da file, ricreate automaticamente):"),
  bulletRuns([R("IoT Sensor Dashboard", { bold: true }), R(" — vista globale sui 4 nodi: card di stato FIRE / NO FIRE, serie storiche di temperatura/CO/fumo, conteggio anomalie, attività incendi.")]),
  bulletRuns([R("IoT Node Detail", { bold: true }), R(" — drill-down su singolo nodo: tutti i sensori sullo stesso asse temporale, andamento z-score, timeline anomalie, statistiche cumulative.")]),
  bulletRuns([R("Mappa Incendi", { bold: true }), R(" — mappa geografica live con marker colorati per stato (verde/arancione/rosso), aggiornamento ogni 5 s, drill-down al singolo nodo (dettagli in §6.1).")]),
  bulletRuns([R("Historical Dashboard", { bold: true }), R(" (bozza) — output del batch layer: percentili, correlazioni, metriche di validazione.")]),

  h2("6.1 Mappa geografica live degli incendi (Geomap)"),
  pRuns([
    R("Una quarta dashboard — "),
    R('"Mappa Incendi"', { bold: true }),
    R(" — mostra i 4 nodi sensore su una mappa geografica reale (basemap "),
    R("OpenStreetMap", { bold: true }),
    R(", pannello nativo "),
    R("Geomap", { bold: true }),
    R(" di Grafana 10.4), con aggiornamento automatico ogni 5 s."),
  ]),
  pRuns([R("Come funziona.  ", { bold: true }), R("Il datasource è l'indice node_status_index di Elasticsearch: un singolo documento per nodo, upsertato a ogni micro-batch. Per supportare la mappa, il job Spark arricchisce quel documento con tre campi aggiuntivi:")]),
  makeTable([2200, 1600, 5226], ["Campo", "Tipo", "Descrizione"], [
    ["lat / lon", "float", "Coordinate geografiche del nodo"],
    ["map_status", "integer", "Livello di stato: 0 = normale · 1 = anomalia · 2 = incendio"],
  ]),
  blank(),
  pRuns([
    R("Il pannello Geomap posiziona i marker usando lat/lon e colora ciascun marker in base a map_status con soglie cromatiche: "),
    R("verde → 0", { bold: true, color: "217346" }),
    R(", "),
    R("arancione → 1", { bold: true, color: "C55A11" }),
    R(", "),
    R("rosso → 2", { bold: true, color: "C00000" }),
    R(". Cliccando un marker appare un tooltip con i valori chiave del nodo (temperatura, CO, fumo, stato); cliccando il node_id si apre la dashboard di dettaglio del singolo nodo."),
  ]),
  pRuns([
    R("Nota metodologica.  ", { bold: true }),
    R("I sensori fisici non hanno GPS; le coordinate sono quindi "),
    R("fittizie", { bold: true }),
    R(", assegnate a mano in una zona boschiva a est di Cagliari (massiccio dei "),
    R("Sette Fratelli", { bold: true }),
    R(", lat ≈ 39.27–39.30, lon ≈ 9.39–9.44). Questo è dichiarato esplicitamente nella description della dashboard e in questa relazione. Il basemap OSM è reale; la mappa è funzionale a livello dimostrativo e richiede connessione internet per i tile."),
  ]),

  // 7
  h1("7. Aspetti “Big Data” enfatizzati"),
  bulletRuns([R("Sorgenti distribuite e parallele", { bold: true }), R(": 4 producer indipendenti, una partizione Kafka ciascuno.")]),
  bulletRuns([R("Elaborazione distribuita reale", { bold: true }), R(": cluster Spark con 3 worker; la logica stateful per nodo gira sugli executor, non sul driver (nessun collect() sui dati di dettaglio).")]),
  bulletRuns([R("Tolleranza ai guasti", { bold: true }), R(": replication-factor 3 + min.insync.replicas 2 → il cluster Kafka regge la perdita di un broker (dimostrabile dal vivo fermando un broker mentre la pipeline continua a ingerire).")]),
  bulletRuns([R("Stato persistente e ripartenza", { bold: true }), R(": lo stato Welford e gli offset Kafka (checkpoint Spark) sopravvivono ai riavvii.")]),
  bulletRuns([R("Persistenza poliglotta", { bold: true }), R(": MongoDB come document store / storico, Elasticsearch come motore time-series per le dashboard.")]),

  // 8
  h1("8. Stato attuale del progetto"),
  pRuns([R("Già funzionante:", { bold: true })]),
  bullet("Cluster Kafka a 3 broker, 4 producer, cluster Spark, MongoDB, Elasticsearch, Grafana, orchestrati con Docker Compose;"),
  bullet("Pipeline streaming completa (cleaning → z-score Welford → soglie → fire transition → multi-sink);"),
  bullet("Seconda query a finestra;"),
  bullet("Batch layer storico con percentili, correlazioni MLlib e validazione precision/recall;"),
  bullet("Dashboard live provisioning-as-code;"),
  bullet("Mappa geografica live (Geomap) con marker per livello di stato, popup dei dati del nodo e drill-down al singolo nodo."),
  pRuns([R("Da rifinire prima della versione finale:", { bold: true })]),
  bullet("Consolidare e documentare i risultati della validazione (numeri di precision/recall per nodo);"),
  bullet("Completare la dashboard storica (batch layer);"),
  bullet("Sezione di valutazione su performance/throughput."),

  // 9
  h1("9. Punti su cui chiedo conferma al docente"),
  numItem([R("Ampiezza dello stack", { bold: true }), R(": l'insieme Kafka + Spark + MongoDB + Elasticsearch + Grafana è adeguato e bilanciato per l'esame, o è preferibile approfondire di più un singolo componente piuttosto che coprirne molti?")]),
  numItem([R("Anomaly detection", { bold: true }), R(": l'approccio (z-score online di Welford + soglie assolute) è sufficiente, oppure ci si aspetta un modello di ML vero e proprio (es. classificatore allenato sul Fire, clustering, Isolation Forest)?")]),
  numItem([R("Architettura Lambda", { bold: true }), R(": la separazione speed/batch è apprezzata o sovradimensionata per la quantità di dati in gioco? Avrebbe senso citare/usare un'impostazione Kappa come alternativa?")]),
  numItem([R("Dati", { bold: true }), R(": ~6 741 letture reali sono sufficienti come volume, o conviene aumentare/sintetizzare il dataset per dare più sostanza alla parte “big”?")]),
  numItem([R("Validazione", { bold: true }), R(": le metriche scelte (precision/recall/F1/accuracy vs ground-truth Fire) sono il taglio giusto per dimostrare la bontà del rilevamento?")]),

  // 10
  h1("10. Possibili estensioni (se richiesto)"),
  bullet("Modello ML supervisionato sul Fire con valutazione train/test (Spark MLlib)."),
  bullet("Alerting end-to-end (il webhook è già predisposto) con notifica reale."),
  bullet("Benchmark di throughput/latenza al variare del numero di worker/partizioni."),
  bullet("Kibana o pannelli aggiuntivi per l'analisi esplorativa su Elasticsearch."),
  bullet("Schema Registry (Avro) per la governance dei messaggi Kafka."),

  new Paragraph({ spacing: { before: 300 }, border: { top: { style: BorderStyle.SINGLE, size: 6, color: "999999", space: 8 } }, children: [
    new TextRun({ text: "Il codice e la configurazione completa (Docker Compose, job Spark, dashboard) sono disponibili nel repository a corredo di questa bozza.", italics: true, size: 20 }),
  ]}),
];

// ── Document ─────────────────────────────────────────────────────────────────
const doc = new Document({
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: "Calibri", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 30, bold: true, color: "1F4E79", font: "Calibri" },
        paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 25, bold: true, color: "2E74B5", font: "Calibri" },
        paragraph: { spacing: { before: 180, after: 100 }, outlineLevel: 1 } },
    ],
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 280 } } } }] },
      { reference: "numbers", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 280 } } } }] },
    ],
  },
  sections: [{
    properties: {
      titlePage: true,
      page: { size: { width: 11906, height: 16838 }, margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } },
    },
    headers: {
      first: new Header({ children: [new Paragraph({ children: [] })] }),
      default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "CCCCCC", space: 4 } }, children: [new TextRun({ text: "Big Data — Bozza di relazione", size: 16, color: "808080" })] })] }),
    },
    footers: {
      first: new Footer({ children: [new Paragraph({ children: [] })] }),
      default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ text: "Pagina ", size: 16, color: "808080" }), new TextRun({ children: [PageNumber.CURRENT], size: 16, color: "808080" })] })] }),
    },
    children: [...titlePage, ...tocBlock, ...body],
  }],
});

Packer.toBuffer(doc).then((buffer) => {
  fs.writeFileSync("BOZZA_RELAZIONE.docx", buffer);
  console.log("OK: BOZZA_RELAZIONE.docx scritto (" + buffer.length + " byte)");
});
