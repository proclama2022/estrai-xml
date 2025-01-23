#!/usr/bin/env python3
import os
import sys
import json
import xml.etree.ElementTree as ET
import argparse
import logging
from datetime import datetime

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fattura_parser.log'),
        logging.StreamHandler()
    ]
)

def parse_fattura(xml_path):
    """Estrae dati strutturati da file XML di Fattura Elettronica"""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Gestione namespace dinamica
        namespace = ''
        if root.tag.startswith("{"):
            namespace = root.tag.split("}")[0].strip("{") + "}"
        
        ns = lambda tag: f'{{{namespace}}}{tag}' if namespace else tag

        # Struttura dati principale
        fattura = {
            'xml_file': os.path.basename(xml_path),
            'header': {
                'supplier': extract_anagrafica(root, ns, 'CedentePrestatore'),
                'customer': extract_anagrafica(root, ns, 'CessionarioCommittente')
            },
            'document_data': extract_dati_generali(root, ns),
            'line_items': extract_line_items(root, ns),
            'payment_details': extract_pagamento(root, ns),
            'tax_summary': extract_riepilogo_iva(root, ns)
        }

        return fattura

    except ET.ParseError as e:
        logging.error(f"Errore parsing XML in {xml_path}: {str(e)}")
        return {'error': 'Invalid XML structure', 'file': xml_path}
    except Exception as e:
        logging.error(f"Errore generico in {xml_path}: {str(e)}", exc_info=True)
        return {'error': str(e), 'file': xml_path}

def extract_anagrafica(root, ns, section):
    """Estrae dati anagrafici completi"""
    element = root.find(f".//{ns('FatturaElettronicaHeader')}/{ns(section)}")
    if element is None:
        return {}
    
    anagrafica = {
        'name': get_text(element, f"{ns('Anagrafica')}/{ns('Denominazione')}"),
        'fiscal_code': get_text(element, ns('CodiceFiscale')),
        'vat_number': get_text(element, f"{ns('IdFiscaleIVA')}/{ns('IdCodice')}"),
        'address': {
            'street': get_text(element, f"{ns('Sede')}/{ns('Indirizzo')}"),
            'postal_code': get_text(element, f"{ns('Sede')}/{ns('CAP')}"),
            'city': get_text(element, f"{ns('Sede')}/{ns('Comune')}"),
            'province': get_text(element, f"{ns('Sede')}/{ns('Provincia')}"),
            'country': get_text(element, f"{ns('Sede')}/{ns('Nazione')}")
        }
    }
    
    # Fallback a Nome+Cognome se manca Denominazione
    if not anagrafica['name']:
        nome = get_text(element, f"{ns('Anagrafica')}/{ns('Nome')}")
        cognome = get_text(element, f"{ns('Anagrafica')}/{ns('Cognome')}")
        anagrafica['name'] = f"{nome} {cognome}".strip()
    
    return clean_data(anagrafica)

def extract_dati_generali(root, ns):
    """Estrae dati generali del documento"""
    dati_gen = root.find(f".//{ns('FatturaElettronicaBody')}/{ns('DatiGenerali')}/{ns('DatiGeneraliDocumento')}")
    if dati_gen is None:
        return {}
    
    return clean_data({
        'type': get_text(dati_gen, ns('TipoDocumento')),
        'number': get_text(dati_gen, ns('Numero')),
        'date': format_date(get_text(dati_gen, ns('Data'))),
        'currency': get_text(dati_gen, ns('Divisa')),
        'total_amount': parse_float(get_text(dati_gen, ns('ImportoTotaleDocumento')))
    })

def extract_line_items(root, ns):
    """Estrae dettaglio linee della fattura"""
    items = []
    for line in root.findall(f".//{ns('FatturaElettronicaBody')}//{ns('DettaglioLinee')}"):
        item = {
            'line_number': parse_int(get_text(line, ns('NumeroLinea'))),
            'description': get_text(line, ns('Descrizione')),
            'quantity': parse_float(get_text(line, ns('Quantita'))),
            'unit_price': parse_float(get_text(line, ns('PrezzoUnitario'))),
            'total_price': parse_float(get_text(line, ns('PrezzoTotale'))),
            'vat_rate': parse_float(get_text(line, ns('AliquotaIVA'))),
            'vat_nature': get_text(line, ns('Natura'))
        }
        items.append(clean_data(item))
    return items

def extract_pagamento(root, ns):
    """Estrae informazioni di pagamento"""
    pagamento = root.find(f".//{ns('DatiPagamento')}")
    if pagamento is None:
        return {}
    
    return clean_data({
        'method': get_text(pagamento, ns('ModalitaPagamento')),
        'terms': get_text(pagamento, ns('TerminiPagamento')),
        'iban': get_text(pagamento, f"{ns('DettaglioPagamento')}/{ns('IBAN')}")
    })

def extract_riepilogo_iva(root, ns):
    """Estrae riepilogo IVA"""
    iva = []
    for riep in root.findall(f".//{ns('DatiRiepilogo')}"):
        iva.append({
            'rate': parse_float(get_text(riep, ns('AliquotaIVA'))),
            'taxable': parse_float(get_text(riep, ns('ImponibileImporto'))),
            'amount': parse_float(get_text(riep, ns('Imposta')))
        })
    return iva

# Funzioni di utilità
def get_text(element, path):
    el = element.find(path) if element is not None else None
    return el.text.strip() if el is not None and el.text else ''

def parse_float(value):
    try:
        return float(value.replace(',', '.')) if value else 0.0
    except ValueError:
        return 0.0

def parse_int(value):
    try:
        return int(value) if value else 0
    except ValueError:
        return 0

def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
    except ValueError:
        return date_str

def clean_data(data):
    """Pulizia ricorsiva degli spazi bianchi e rimozione valori vuoti"""
    if isinstance(data, dict):
        return {k: clean_data(v) for k, v in data.items() if v not in ['', None, 0]}
    elif isinstance(data, list):
        return [clean_data(item) for item in data]
    elif isinstance(data, str):
        return data.strip()
    return data

def main():
    parser = argparse.ArgumentParser(description='Parser Fatture Elettroniche XML')
    parser.add_argument('files', nargs='+', help='File XML da processare')
    parser.add_argument('-o', '--output', default='fatture_output.json', 
                      help='File JSON di output')
    parser.add_argument('-v', '--verbose', action='store_true',
                      help='Logging dettagliato')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    results = []
    for f in args.files:
        if os.path.isfile(f) and f.lower().endswith('.xml'):
            logging.info(f"Processing: {f}")
            result = parse_fattura(f)
            if 'error' not in result:
                results.append(result)
            else:
                logging.warning(f"Skipped invalid file: {f}")
        else:
            logging.warning(f"Ignored non-XML file: {f}")
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    logging.info(f"Output salvato in: {args.output}")

if __name__ == "__main__":
    main()