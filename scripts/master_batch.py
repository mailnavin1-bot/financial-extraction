"""
Master Batch Orchestrator
Watches input directory, auto-renames files, and runs the pipeline end-to-end.
"""
import os
import time
import shutil
import subprocess
import logging
from pathlib import Path
import google.generativeai as genai
# Import utils from current package context
try:
    from utils import load_settings, setup_logging
except ImportError:
    # Fallback if running directly from scripts folder
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from utils import load_settings, setup_logging

# Setup logging
logger = setup_logging("logs", logging.INFO)

def get_pdf_metadata(file_path):
    """
    Uses Gemini to peek at the PDF and extract Company/Year for renaming.
    """
    settings = load_settings(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'settings.json'))
    genai.configure(api_key=settings['gemini_api_key'])
    
    try:
        # Upload file to Gemini (temp)
        sample_file = genai.upload_file(file_path, mime_type="application/pdf")
        
        # Wait a moment for processing if needed
        time.sleep(2)
        
        model = genai.GenerativeModel("gemini-2.0-flash")
        prompt = """
        Look at the first few pages of this document. 
        Extract the 'Company Name' and the 'Fiscal Year' (e.g. 2024 for FY23-24).
        Return ONLY a JSON string: {"company": "Name", "year": "YYYY"}
        Do not add markdown formatting like ```json.
        """
        
        result = model.generate_content([sample_file, prompt])
        response_text = result.text.replace('```json', '').replace('```', '').strip()
        
        import json
        data = json.loads(response_text)
        
        # Cleanup
        sample_file.delete()
        
        return data['company'], str(data['year'])
        
    except Exception as e:
        logger.error(f"Auto-naming failed for {file_path}: {e}")
        return None, None

def sanitize_filename(company, year):
    """Creates Standardized Filename: Company_AR_YYYY.pdf"""
    # Keep only alphanumeric and standard separators
    clean_company = "".join(x for x in company if x.isalnum() or x in [' ', '-', '_']).strip()
    clean_company = clean_company.replace(" ", "_")
    return f"{clean_company}_AR_{year}.pdf"

def run_pipeline(pdf_path):
    """Calls the PowerShell pipeline for a single file"""
    logger.info(f">> Starting Pipeline for: {os.path.basename(pdf_path)}")
    
    # Construct absolute paths
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(root_dir, "scripts", "run_pipeline.ps1")
    
    cmd = [
        "powershell", "-ExecutionPolicy", "Bypass", 
        "-File", script_path, 
        "-PDFPath", pdf_path
    ]
    
    try:
        # Run and stream output
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        
        # Stream stdout in real-time
        for line in iter(process.stdout.readline, ''):
            print(line.strip())
            
        process.wait()
        
        if process.returncode == 0:
            logger.info(f"OK: Pipeline Success: {os.path.basename(pdf_path)}")
            return True
        else:
            stderr_output = process.stderr.read()
            logger.error(f"FAIL: Pipeline Failed: {os.path.basename(pdf_path)}")
            logger.error(f"Error details: {stderr_output}")
            return False
            
    except Exception as e:
        logger.error(f"Pipeline Execution Error: {e}")
        return False

def main():
    # Load settings from relative path
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    settings_path = os.path.join(root_dir, 'config', 'settings.json')
    settings = load_settings(settings_path)
    
    input_dir = os.path.join(root_dir, settings['paths']['input_pdfs'])
    processed_dir = os.path.join(input_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    
    logger.info(f"Watching directory: {input_dir}")
    
    # Get all PDFs in root of input_dir
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    
    if not files:
        logger.warning("No PDF files found in input directory.")
        return

    logger.info(f"Found {len(files)} files to process.")

    for filename in files:
        original_path = os.path.join(input_dir, filename)
        
        # 1. Check if renaming is needed
        # Logic: If it doesn't match our specific pattern OR if user wants full automation
        if "_AR_20" not in filename:
            logger.info(f"Inspect file for auto-naming: {filename}")
            company, year = get_pdf_metadata(original_path)
            
            if company and year:
                new_name = sanitize_filename(company, year)
                new_path = os.path.join(input_dir, new_name)
                
                if original_path != new_path:
                    try:
                        os.rename(original_path, new_path)
                        logger.info(f"Renamed: {filename} -> {new_name}")
                        filename = new_name
                        original_path = new_path
                    except OSError as e:
                        logger.error(f"Could not rename file: {e}")
            else:
                logger.warning(f"Could not auto-name {filename}. Proceeding with original name...")

        # 2. Run Pipeline
        success = run_pipeline(original_path)
        
        # 3. Move to Processed Folder
        if success:
            try:
                shutil.move(original_path, os.path.join(processed_dir, filename))
                logger.info(f"Moved {filename} to processed folder.")
            except Exception as e:
                logger.error(f"Failed to move file to processed folder: {e}")

if __name__ == "__main__":
    main()