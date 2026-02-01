"""
Shared utility functions for the extraction pipeline
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import uuid
from colorlog import ColoredFormatter

def setup_logging(log_dir="logs", log_level=logging.INFO):
    """
    Setup colored logging
    """
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def load_settings(config_path="config/settings.json"):
    """
    Load configuration settings
    """
    with open(config_path, 'r') as f:
        return json.load(f)

def load_prompt_template(prompt_path="config/universal_prompt.txt"):
    """
    Load universal prompt template
    """
    with open(prompt_path, 'r', encoding='utf-8') as f:
        return f.read()

def parse_pdf_filename(filename):
    """
    Parse PDF filename to extract company and year
    Expected format: CompanyName_AR_2024.pdf
    
    Returns:
        dict: {'company': 'TCS', 'year': 2024, 'valid': True}
    """
    try:
        # Remove .pdf extension
        name = filename.replace('.pdf', '').replace('.PDF', '')
        
        # Split by _AR_
        parts = name.split('_AR_')
        
        if len(parts) != 2:
            return {'valid': False, 'error': 'Format should be CompanyName_AR_YYYY.pdf'}
        
        company = parts[0].replace('_', ' ').strip()
        year = int(parts[1])
        
        if year < 2000 or year > 2030:
            return {'valid': False, 'error': f'Invalid year: {year}'}
        
        return {
            'valid': True,
            'company': company,
            'year': year,
            'document_id': f"{company.replace(' ', '_')}_AR_{year}"
        }
        
    except Exception as e:
        return {'valid': False, 'error': str(e)}

def create_output_directories(base_path="output"):
    """
    Create all required output directories
    """
    dirs = [
        "stage0_structure",
        "stage1_flagged_pages",
        "stage2_images",
        "stage3_extractions",
        "stage4_consolidated",
        "stage4_5_filtered",
        "stage5_verified",
        "stage6_gemini_reviewed",
        "final"
    ]
    
    for d in dirs:
        os.makedirs(os.path.join(base_path, d), exist_ok=True)

def generate_extraction_id():
    """
    Generate unique extraction ID
    """
    return str(uuid.uuid4())

def save_json(data, filepath):
    """
    Save data to JSON file with pretty printing
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_json(filepath):
    """
    Load JSON file
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_company_metadata(company_name, metadata_csv="config/company_metadata.csv"):
    """
    Get company metadata from CSV
    """
    import pandas as pd
    
    if not os.path.exists(metadata_csv):
        return {
            'industry': 'Unknown',
            'fiscal_year_end': 'March 31'
        }
    
    df = pd.read_csv(metadata_csv)
    
    # Try exact match
    match = df[df['company_name'].str.lower() == company_name.lower()]
    
    if len(match) == 0:
        # Try partial match
        match = df[df['company_name'].str.lower().str.contains(company_name.lower())]
    
    if len(match) > 0:
        return {
            'industry': match.iloc[0]['industry'],
            'fiscal_year_end': match.iloc[0]['fiscal_year_end']
        }
    
    return {
        'industry': 'Unknown',
        'fiscal_year_end': 'March 31'
    }

def estimate_processing_cost(num_pages):
    """
    Estimate processing cost for a document
    """
    # Stage 1: ~$0.006 per document
    stage1_cost = 0.006
    
    # Stage 3: ~$0.012 per document (based on 40 pages)
    stage3_cost = 0.012 * (num_pages / 40)
    
    # Stage 5: ~$0.006 per document
    stage5_cost = 0.006
    
    # Stage 6: ~$0.15 per document (Gemini, only 20% of extractions)
    stage6_cost = 0.15 * 0.2
    
    total = stage1_cost + stage3_cost + stage5_cost + stage6_cost
    
    return {
        'stage1': round(stage1_cost, 4),
        'stage3': round(stage3_cost, 4),
        'stage5': round(stage5_cost, 4),
        'stage6': round(stage6_cost, 4),
        'total': round(total, 4)
    }
