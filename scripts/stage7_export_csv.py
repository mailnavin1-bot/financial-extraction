"""
Stage 7: Export to CSV
Convert final JSON to CSV format for BigQuery loading
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, create_output_directories

logger = logging.getLogger(__name__)

class CSVExporter:
    """
    Export extractions to CSV
    """
    
    def __init__(self, final_json_path):
        self.data = load_json(final_json_path)
        self.document_id = self.data['document_id']
        self.extractions = self.data['extractions']
    
    def export_to_csv(self):
        """
        Convert extractions to CSV format
        """
        logger.info(f"Exporting {len(self.extractions)} extractions to CSV...")
        
        rows = []
        
        for extraction in self.extractions:
            row = self._flatten_extraction(extraction)
            rows.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Reorder columns for readability
        column_order = [
            'extraction_id', 'document_id', 'company_name', 'industry',
            'report_year', 'fiscal_year', 'is_current_report_year',
            'kpi_name', 'kpi_description', 'kpi_category',
            'value_raw', 'value_numeric', 'value_actual',
            'unit', 'currency', 'magnitude_unit',
            'source_section', 'source_page', 'source_table_title',
            'source_column_label', 'multi_year_table',
            'confidence', 'confidence_reasoning',
            'verification_status', 'verification_confidence',
            'gemini_reviewed', 'gemini_decision',
            'needs_review', 'extraction_method', 'extracted_at'
        ]
        
        # Only include columns that exist
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        return df
    
    def _flatten_extraction(self, extraction):
        """
        Flatten nested extraction JSON to flat row
        """
        row = {
            'extraction_id': extraction.get('extraction_id', ''),
            'document_id': self.document_id,
            'company_name': extraction.get('page_metadata', {}).get('company', ''),
            'industry': extraction.get('page_metadata', {}).get('industry', ''),
            'report_year': extraction.get('report_year', 0),
            'fiscal_year': extraction.get('fiscal_year', 0),
            'is_current_report_year': extraction.get('is_current_report_year', False),
            
            'kpi_name': extraction.get('kpi_name', ''),
            'kpi_description': extraction.get('kpi_description', ''),
            'kpi_category': extraction.get('kpi_category', ''),
            
            'value_raw': extraction.get('value_raw', ''),
            'value_numeric': extraction.get('value_numeric'),
            'value_actual': extraction.get('value_actual'),
            'unit': extraction.get('unit', ''),
            'currency': extraction.get('currency', ''),
            'magnitude_unit': extraction.get('magnitude_unit', ''),
            
            'source_section': extraction.get('source', {}).get('section', ''),
            'source_page': extraction.get('source', {}).get('page', 0),
            'source_table_title': extraction.get('source', {}).get('table_title', ''),
            'source_column_label': extraction.get('source', {}).get('column_label', ''),
            
            'multi_year_table': extraction.get('context', {}).get('multi_year_table', False),
            
            'confidence': extraction.get('confidence', 0),
            'confidence_reasoning': extraction.get('confidence_reasoning', ''),
            
            'verification_status': extraction.get('verification_status', ''),
            'verification_confidence': extraction.get('verification_confidence', 0),
            
            'gemini_reviewed': extraction.get('gemini_reviewed', False),
            'gemini_decision': extraction.get('gemini_decision', ''),
            
            'needs_review': extraction.get('flags', {}).get('needs_review', False),
            
            'extraction_method': 'qwen72b_verified',
            'extracted_at': datetime.now().isoformat()
        }
        
        return row

def main(final_json_path, output_dir="output/final", append_to_master=True):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 7: CSV EXPORT")
    logger.info(f"=" * 60)
    
    exporter = CSVExporter(final_json_path)
    df = exporter.export_to_csv()
    
    # Save individual document CSV
    output_file = os.path.join(output_dir, f"{exporter.document_id}_extractions.csv")
    df.to_csv(output_file, index=False)
    
    logger.info(f"✓ CSV saved: {output_file}")
    logger.info(f"  Rows: {len(df)}")
    logger.info(f"  Columns: {len(df.columns)}")
    
    # Append to master CSV
    if append_to_master:
        master_file = os.path.join(output_dir, "extractions_for_bq.csv")
        
        if os.path.exists(master_file):
            # Append
            df.to_csv(master_file, mode='a', header=False, index=False)
            logger.info(f"✓ Appended to master CSV: {master_file}")
        else:
            # Create new
            df.to_csv(master_file, index=False)
            logger.info(f"✓ Created master CSV: {master_file}")
    
    # Summary statistics
    logger.info(f"\nSummary:")
    logger.info(f"  Unique KPIs: {df['kpi_name'].nunique()}")
    logger.info(f"  Fiscal years: {sorted(df['fiscal_year'].unique())}")
    logger.info(f"  Avg confidence: {df['confidence'].mean():.3f}")
    logger.info(f"  Needs review: {df['needs_review'].sum()}")
    
    return df

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Export to CSV')
    parser.add_argument('final_json', help='Path to final JSON from Stage 6')
    parser.add_argument('--output-dir', default='output/final')
    parser.add_argument('--no-append', action='store_true', 
                       help='Do not append to master CSV')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.final_json, args.output_dir, append_to_master=not args.no_append)
