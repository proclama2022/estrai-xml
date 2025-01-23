import streamlit as st
import json
import os
import tempfile
from fattura_elettronica_parser_advanced import InvoiceProcessor

def process_fattura(uploaded_files):
    if not uploaded_files:
        return None, "Nessun file caricato."

    temp_dir = tempfile.TemporaryDirectory()
    input_paths = []
    
    # Salva i file caricati in directory temporanea
    for uploaded_file in uploaded_files:
        temp_file_path = os.path.join(temp_dir.name, uploaded_file.name)
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(uploaded_file.read())
        input_paths.append(temp_file_path)

    try:
        # Inizializza il processor
        processor = InvoiceProcessor()
        
        # Elabora i file
        valid, errors = processor.process_files(
            input_paths,
            output_format='json',
            output_base=os.path.join(temp_dir.name, "output")
        )
        
        # Leggi il risultato JSON
        output_json_path = os.path.join(temp_dir.name, "output.json")
        if os.path.exists(output_json_path):
            with open(output_json_path, "r", encoding="utf-8") as f:
                json_output = json.load(f)
            return json_output, None
        else:
            return None, f"File JSON di output non trovato. Elaborati {valid} file con successo e {errors} errori."

    except Exception as e:
        error_message = f"Errore imprevisto:\n{str(e)}"
        return None, error_message
    finally:
        temp_dir.cleanup()  # Pulisci la directory temporanea


st.title("Estrattore Fatture Elettroniche Italiane")

uploaded_files = st.file_uploader(
    "Carica file XML o ZIP fatture elettroniche",
    type=["xml", "zip"],
    accept_multiple_files=True
)

if st.button("Processa Fatture"):
    if uploaded_files:
        json_output, error_message = process_fattura(uploaded_files)
        if json_output:
            st.subheader("Output JSON:")
            st.json(json_output)
            st.download_button(
                label="Scarica JSON",
                data=json.dumps(json_output, indent=2, ensure_ascii=False).encode('utf-8'),
                file_name="fatture_output.json",
                mime="application/json",
            )
        else:
            st.error(f"Errore durante l'elaborazione:\n{error_message}")
    else:
        st.warning("Carica almeno un file per iniziare l'elaborazione.")