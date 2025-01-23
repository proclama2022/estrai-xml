#!/usr/bin/env python3
import os
import sys
import json
import argparse
import logging
import multiprocessing
import zipfile
import tempfile
from datetime import datetime
from collections import defaultdict
from defusedxml.ElementTree import parse as safe_parse
import xml.etree.ElementTree as ET
import pandas as pd
import yaml

# Configurazione avanzata
CONFIG = {
    'schema_registry': {
        'base': 'https://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2',
        'custom_schemas': {
            'v1.2': 'FatturaPA v1.2.2.xsd'
        }
    },
    'error_recovery': {
        'common_issues': {
            'encoding': ['iso-8859-15', 'windows-1252'],
            'malformed_tags': ['&', '<']
        }
    },
    'data_normalization': {
        'currency': 'EUR',
        'date_format': '%Y-%m-%d',
        'default_vat_rate': 22.0
    }
}

# Setup logging avanzato
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d|%(module)s|%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('fattura_parser_advanced.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class InvoiceProcessor:
    """Classe principale per l'elaborazione avanzata delle fatture"""
    
    def __init__(self, config=None):
        self.config = config or CONFIG
        self.schema_cache = {}
        self.error_stats = defaultdict(int)

    def handle_error(self, xml_path, error_type, error_details):
        """Gestione centralizzata degli errori"""
        error_msg = f"Errore {error_type} in {xml_path}: {error_details}"
        logging.error(error_msg)
        self.error_stats[error_type] += 1
        return {
            'status': 'error',
            'file': xml_path,
            'error_type': error_type,
            'error_details': error_details
        }

    def process_files(self, files, output_format='json', output_base='output'):
        """Elaborazione parallela di file/directory/ZIP"""
        xml_files = []
        
        for file_path in files:
            if zipfile.is_zipfile(file_path):
                xml_files.extend(self.extract_zip(file_path))
            elif os.path.isdir(file_path):
                xml_files.extend(self.get_xml_from_dir(file_path))
            elif file_path.lower().endswith('.xml'):
                xml_files.append(file_path)
            else:
                logging.warning(f"Ignorato file non supportato: {file_path}")
        
        if not xml_files:
            logging.error("Nessun file XML valido trovato")
            return 0, len(files)
            
        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            results = pool.map(self.process_single, xml_files)
        
        valid_invoices = [r for r in results if r['status'] == 'success']
        failed_invoices = [r for r in results if r['status'] == 'error']
        
        self.generate_output(valid_invoices, output_format)
        self.generate_error_report(failed_invoices, output_base)
        return len(valid_invoices), len(failed_invoices)

    def process_single(self, xml_path):
        """Elaborazione singolo file con recovery avanzato"""
        try:
            # Verifica esistenza file
            if not os.path.exists(xml_path):
                return self.handle_error(xml_path, 'file_not_found', 'File non trovato')
                
            # Verifica dimensione file
            if os.path.getsize(xml_path) == 0:
                return self.handle_error(xml_path, 'empty_file', 'File vuoto')
            
            # Parsing diretto del file con defusedxml
            try:
                root = safe_parse(xml_path).getroot()
            except Exception as e:
                return self.handle_error(xml_path, 'xml_parsing', f'Errore parsing XML: {str(e)}')
                
            try:
                invoice_data = self.parse_xml(root)
                normalized_data = self.normalize_data(invoice_data)
                
                return {
                    'status': 'success',
                    'data': normalized_data,
                    'metrics': self.calculate_metrics(normalized_data)
                }
                
            except Exception as e:
                return self.handle_error(xml_path, 'data_processing', f'Errore elaborazione dati: {str(e)}')
            
        except Exception as e:
            return self.handle_error(xml_path, 'unexpected_error', f'Errore imprevisto: {str(e)}')

    def preprocess_xml(self, xml_path):
        """Correzioni automatiche pre-parsing"""
        try:
            with open(xml_path, 'rb') as f:
                raw_data = f.read()
                
            # Rimuovi BOM se presente
            if raw_data.startswith(b'\xef\xbb\xbf'):
                raw_data = raw_data[3:]
                
            # Correzione encoding
            for encoding in self.config['error_recovery']['common_issues']['encoding']:
                try:
                    return raw_data.decode(encoding).encode('utf-8')
                except UnicodeDecodeError:
                    continue
                    
            # Correzione caratteri speciali
            corrected = raw_data
            for bad, good in self.config['error_recovery']['common_issues']['malformed_tags']:
                corrected = corrected.replace(bad.encode(), good.encode())
                
            return corrected
            
        except Exception as e:
            return self.handle_error(xml_path, 'preprocessing_io', str(e))

    def validate_xml(self, xml_data):
        """Validazione XML"""
        return []  # Validazione temporaneamente disabilitata

    def parse_xml(self, root):
        """Parsing strutturato con fallback intelligente"""
        # Namespace predefinito per FatturaPA
        ns_map = {
            'ns': 'http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2',
            'ds': 'http://www.w3.org/2000/09/xmldsig#',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }
        
        # Funzione helper per ricerca elementi
        def find_element(parent, tag):
            return parent.find(tag, namespaces=ns_map) or parent.find(f'*[local-name()="{tag.split("}")[-1]}"]')
        
        return {
            'header': self.parse_header(root, ns_map),
            'document': self.parse_document(root, ns_map),
            'line_items': self.parse_line_items(root, ns_map),
            'payment': self.parse_payment(root, ns_map),
            'tax': self.parse_tax(root, ns_map)
        }

    def parse_header(self, root, ns_map):
        """Estrazione dati intestazione"""
        header = root.find('ns:FatturaElettronicaHeader', ns_map)
        if header is None:
            header = root.find('*[local-name()="FatturaElettronicaHeader"]')
            
        if header is None:
            return {'supplier': {}, 'customer': {}}
            
        return {
            'supplier': self.extract_party_data(header, 'ns:CedentePrestatore', ns_map),
            'customer': self.extract_party_data(header, 'ns:CessionarioCommittente', ns_map)
        }

    def extract_party_data(self, root, xpath, ns_map):
        """Estrazione dati anagrafici generici"""
        party = root.findall(xpath)
        if not party:
            return {}
            
        party = party[0]
        return {
            'name': self.xpath_text(party, './/*[local-name()="Anagrafica"]/*[local-name()="Denominazione"]', ns_map),
            'vat': self.xpath_text(party, './/*[local-name()="IdFiscaleIVA"]/*[local-name()="IdCodice"]', ns_map),
            'address': {
                'street': self.xpath_text(party, './/*[local-name()="Sede"]/*[local-name()="Indirizzo"]', ns_map),
                'zip': self.xpath_text(party, './/*[local-name()="Sede"]/*[local-name()="CAP"]', ns_map),
                'city': self.xpath_text(party, './/*[local-name()="Sede"]/*[local-name()="Comune"]', ns_map),
                'country': self.xpath_text(party, './/*[local-name()="Sede"]/*[local-name()="Nazione"]', ns_map)
            }
        }

    def parse_document(self, root, ns_map):
        """Estrazione dati documento principale"""
        return {
            'type': self.xpath_text(root, '//*[local-name()="DatiGeneraliDocumento"]/*[local-name()="TipoDocumento"]', ns_map),
            'number': self.xpath_text(root, '//*[local-name()="DatiGeneraliDocumento"]/*[local-name()="Numero"]', ns_map),
            'date': self.parse_date(self.xpath_text(root, '//*[local-name()="DatiGeneraliDocumento"]/*[local-name()="Data"]', ns_map)),
            'currency': self.xpath_text(root, '//*[local-name()="DatiGeneraliDocumento"]/*[local-name()="Divisa"]', ns_map),
            'total': self.parse_float(self.xpath_text(root, '//*[local-name()="DatiGeneraliDocumento"]/*[local-name()="ImportoTotaleDocumento"]', ns_map))
        }

    def parse_line_items(self, root, ns_map):
        """Estrazione righe dettaglio"""
        return [self.parse_line(line, ns_map) 
                for line in root.findall('//*[local-name()="FatturaElettronicaBody"]/*[local-name()="DatiBeniServizi"]/*[local-name()="DettaglioLinee"]')]

    def parse_line(self, line, ns_map):
        """Parsing singola riga"""
        return {
            'line_number': self.parse_int(self.xpath_text(line, './/*[local-name()="NumeroLinea"]', ns_map)),
            'description': self.xpath_text(line, './/*[local-name()="Descrizione"]', ns_map),
            'quantity': self.parse_float(self.xpath_text(line, './/*[local-name()="Quantita"]', ns_map)),
            'price': self.parse_float(self.xpath_text(line, './/*[local-name()="PrezzoUnitario"]', ns_map)),
            'total': self.parse_float(self.xpath_text(line, './/*[local-name()="PrezzoTotale"]', ns_map)),
            'vat_rate': self.parse_float(self.xpath_text(line, './/*[local-name()="AliquotaIVA"]', ns_map))
        }

    def parse_payment(self, root, ns_map):
        """Estrazione dati pagamento"""
        return {}

    def parse_tax(self, root, ns_map):
        """Estrazione dati riepilogo IVA"""
        return {}

    def xpath_text(self, element, xpath, ns_map):
        """Estrazione testo da elemento XML via XPath con namespace"""
        try:
            return element.find(xpath).text.strip()
        except (IndexError, AttributeError):
            return ''

    def parse_float(self, value):
        """Conversione stringa a float gestendo la virgola"""
        try:
            return float(value.replace(',', '.')) if value else 0.0
        except ValueError:
            return 0.0

    def parse_int(self, value):
        """Conversione stringa a int"""
        try:
            return int(value) if value else 0
        except ValueError:
            return 0

    def extract_zip(self, zip_path):
        """Estrai file XML da archivio ZIP"""
        xml_files = []
        try:
            with zipfile.ZipFile(zip_path) as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.lower().endswith('.xml'):
                        with zip_ref.open(file_info) as xml_file:
                            xml_content = xml_file.read()
                            temp_path = os.path.join(tempfile.gettempdir(), file_info.filename)
                            with open(temp_path, 'wb') as temp_file:
                                temp_file.write(xml_content)
                            xml_files.append(temp_path)
        except zipfile.BadZipFile:
            logging.error(f"File ZIP corrotto: {zip_path}")
        except Exception as e:
            logging.error(f"Errore elaborazione ZIP {zip_path}: {str(e)}")
        return xml_files

    def get_xml_from_dir(self, dir_path):
        """Recupera file XML da directory"""
        xml_files = []
        try:
            for root, _, files in os.walk(dir_path):
                for file in files:
                    if file.lower().endswith('.xml'):
                        xml_files.append(os.path.join(root, file))
        except Exception as e:
            logging.error(f"Errore lettura directory {dir_path}: {str(e)}")
        return xml_files

    def parse_date(self, date_str):
        """Conversione stringa data a formato ISO"""
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
        except (ValueError, TypeError):
            return None

    def normalize_data(self, raw_data):
        """Normalizzazione dati secondo regole aziendali"""
        normalized = raw_data.copy()
        
        # Normalizza date
        if 'document' in normalized:
            normalized['document']['date'] = self.normalize_date(normalized['document'].get('date'))
        
        # Normalizza valuta
        if 'document' in normalized:
            if not normalized['document'].get('currency'):
                normalized['document']['currency'] = self.config['data_normalization']['currency']
        
        # Normalizza aliquote IVA
        if 'line_items' in normalized:
            for item in normalized['line_items']:
                if not item.get('vat_rate'):
                    item['vat_rate'] = self.config['data_normalization']['default_vat_rate']
        
        return normalized

    def normalize_date(self, date_value):
        """Normalizzazione date in formato standard"""
        if isinstance(date_value, str):
            try:
                return datetime.strptime(date_value, '%Y-%m-%d').strftime(self.config['data_normalization']['date_format'])
            except ValueError:
                return date_value
        return date_value

    def generate_output(self, invoices, output_format, output_base='output'):
        """Generazione output in formato JSON o CSV"""
        if not invoices:
            logging.warning("Nessuna fattura valida da salvare.")
            return
        if output_format == 'json':
            output_file = f"{output_base}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([inv['data'] for inv in invoices], f, indent=2, ensure_ascii=False, default=str)
            logging.info(f"Output JSON salvato in: {output_file}")
        elif output_format == 'csv':
            output_file = f"{output_base}.csv"
            df = pd.DataFrame([inv['data'] for inv in invoices])
            df.to_csv(output_file, index=False, encoding='utf-8')
            logging.info(f"Output CSV salvato in: {output_file}")

        # Genera file metriche
        metrics_file = f"{output_base}_metrics.csv"
        metrics_df = pd.DataFrame([inv['metrics'] for inv in invoices])
        metrics_df.to_csv(metrics_file, index=False, encoding='utf-8')
        logging.info(f"Metriche salvate in: {metrics_file}")

    def generate_error_report(self, failed_invoices, output_base='output'):
        """Genera report dettagliato degli errori in formato JSON"""
        if not failed_invoices:
            logging.info("Nessun errore riscontrato durante l'elaborazione.")
            return

        error_file = f"{output_base}_errors.json"
        try:
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(failed_invoices, f, indent=2, ensure_ascii=False)
            logging.warning(f"Report errori JSON salvato in: {error_file}")
        except Exception as e:
            logging.error(f"Errore durante il salvataggio del report errori: {str(e)}")

    def calculate_metrics(self, invoice_data):
        """Calcola metriche di qualità dei dati estratti"""
        metrics = {}
        line_items = invoice_data.get('line_items', [])
        metrics['line_items_count'] = len(line_items)
        metrics['total_gross_amount'] = sum(item.get('total', 0) for item in line_items)
        metrics['vat_summary_available'] = bool(invoice_data.get('tax'))
        return metrics


def main():
    parser = argparse.ArgumentParser(
        description='Advanced Italian E-Invoice Processor',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('input', nargs='+', help='File o directory da processare')
    parser.add_argument('-o', '--output', default='output', 
                      help='Prefisso file di output')
    parser.add_argument('-f', '--format', choices=['json', 'csv'], default='json',
                      help='Formato di output')
    parser.add_argument('-c', '--config', help='File di configurazione YAML')
    parser.add_argument('--parallel', type=int, default=0,
                      help='Numero di processi paralleli (0=auto)')
    
    global args
    args = parser.parse_args()
    
    processor = InvoiceProcessor()
    valid, errors = processor.process_files(args.input, args.format)
    
    logging.info(f"Elaborazione completata: {valid} successi, {errors} errori")

if __name__ == "__main__":
    main()
