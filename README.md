
# Estrattore Fatture Elettroniche

Un'applicazione Streamlit per l'elaborazione e l'analisi delle fatture elettroniche italiane.

## Funzionalità

- Supporto per file XML singoli e archivi ZIP
- Parsing avanzato con gestione degli errori
- Estrazione dati strutturati in formato JSON/CSV
- Interfaccia web intuitiva con Streamlit
- Supporto per il processing in parallelo
- Generazione di report dettagliati

## Requisiti

```bash
pip install -r requirements.txt
```

## Utilizzo

### Via Streamlit

```bash
streamlit run fattura_streamlit_app.py
```

### Via Command Line

```bash
python fattura_elettronica_parser_advanced.py input_file.xml -o output
```

Opzioni disponibili:
- `-o, --output`: Prefisso file di output (default: output)
- `-f, --format`: Formato di output (json/csv, default: json)
- `--parallel`: Numero di processi paralleli (0=auto)

## Struttura Output

### JSON
```json
{
    "header": {
        "supplier": {...},
        "customer": {...}
    },
    "document": {
        "type": "...",
        "number": "...",
        "date": "...",
        "currency": "EUR",
        "total": 0.0
    },
    "line_items": [
        {
            "line_number": 1,
            "description": "...",
            "quantity": 0.0,
            "price": 0.0,
            "total": 0.0,
            "vat_rate": 22.0
        }
    ]
}
```

## Sicurezza

- Utilizzo di defusedxml per il parsing sicuro
- Validazione input
- Gestione errori robusta
