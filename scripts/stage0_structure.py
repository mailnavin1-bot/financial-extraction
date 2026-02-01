"""
Stage 0: Document Structure Extraction
Extract PDF structure using bookmarks, ToC, or headers
"""

import os
import sys
import logging
from pathlib import Path
import pdfplumber
import fitz  # PyMuPDF
import re
from datetime import datetime

# Add parent directory to path to allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import (
    setup_logging, parse_pdf_filename, save_json, 
    create_output_directories, get_company_metadata
)

logger = logging.getLogger(__name__)

class StructureExtractor:
    """
    Extract document structure from PDF
    """
    
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.filename = os.path.basename(pdf_path)
        self.parsed_filename = parse_pdf_filename(self.filename)
        
        if not self.parsed_filename['valid']:
            raise ValueError(f"Invalid filename: {self.parsed_filename['error']}")
        
        self.document_id = self.parsed_filename['document_id']
        self.company = self.parsed_filename['company']
        self.report_year = self.parsed_filename['year']
        
        # Get company metadata
        self.metadata = get_company_metadata(self.company)
        
    def extract_structure(self):
        """
        Main extraction method - try multiple approaches
        """
        logger.info(f"Starting structure extraction for {self.document_id}")
        
        # Try methods in order of reliability
        structure = None
        
        # Method 1: PDF Bookmarks
        structure = self._extract_from_bookmarks()
        if structure:
            logger.info("Structure extracted from PDF bookmarks")
            return structure
        
        # Method 2: Table of Contents parsing
        structure = self._extract_from_toc()
        if structure:
            logger.info("Structure extracted from Table of Contents")
            return structure
        
        # Method 3: Header analysis
        structure = self._extract_from_headers()
        if structure:
            logger.info("Structure extracted from headers")
            return structure
        
        # Fallback: Full scan
        logger.warning("Using fallback: full document scan")
        return self._fallback_structure()
    
    def _extract_from_bookmarks(self):
        """
        Extract structure from PDF bookmarks/outline
        """
        try:
            doc = fitz.open(self.pdf_path)
            toc = doc.get_toc()
            
            if not toc or len(toc) == 0:
                return None
            
            sections = []
            for i, (level, title, page) in enumerate(toc):
                if level == 1:  # Top-level sections only
                    # Determine end page
                    if i + 1 < len(toc):
                        next_page = toc[i + 1][2]
                    else:
                        next_page = len(doc)
                    
                    sections.append({
                        'name': title.strip(),
                        'page_start': page,
                        'page_end': next_page - 1,
                        'blacklisted': self._is_financial_statement(title),
                        'category': self._categorize_section(title)
                    })
            
            doc.close()
            
            return self._build_structure_dict(sections, 'pdf_bookmarks', len(doc))
        
        except Exception as e:
            logger.debug(f"Bookmark extraction failed: {e}")
            return None
    
    def _extract_from_toc(self):
        """
        Extract structure from Table of Contents pages
        """
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                # Scan pages 2-10 for ToC
                for page_num in range(2, min(11, len(pdf.pages) + 1)):
                    page = pdf.pages[page_num - 1]
                    text = page.extract_text()
                    
                    if not text:
                        continue
                    
                    # Look for ToC patterns
                    # Pattern: "Section Name ........ Page 24"
                    pattern = r'(.+?)\s*\.{2,}\s*(\d+)'
                    matches = re.findall(pattern, text, re.MULTILINE)
                    
                    if len(matches) > 5:  # Likely a ToC
                        sections = []
                        for title, page in matches:
                            sections.append({
                                'name': title.strip(),
                                'page_start': int(page),
                                'page_end': int(page),  # Will be updated
                                'blacklisted': self._is_financial_statement(title),
                                'category': self._categorize_section(title)
                            })
                        
                        # Update end pages
                        for i in range(len(sections) - 1):
                            sections[i]['page_end'] = sections[i + 1]['page_start'] - 1
                        sections[-1]['page_end'] = len(pdf.pages)
                        
                        return self._build_structure_dict(sections, 'toc_parsing', len(pdf.pages))
            
            return None
        
        except Exception as e:
            logger.debug(f"ToC extraction failed: {e}")
            return None
    
    def _extract_from_headers(self):
        """
        Extract structure from page headers
        """
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                headers = {}
                current_section = None
                
                # Scan first 100 pages (or all if less)
                for page_num in range(1, min(101, len(pdf.pages) + 1)):
                    page = pdf.pages[page_num - 1]
                    
                    # Extract top 10% of page (header area)
                    bbox = (0, 0, page.width, page.height * 0.1)
                    header_text = page.within_bbox(bbox).extract_text()
                    
                    if header_text:
                        header_text = header_text.strip()
                        
                        # Check if header changed (new section)
                        if header_text != current_section and len(header_text) > 5:
                            if current_section is not None:
                                headers[current_section]['page_end'] = page_num - 1
                            
                            headers[header_text] = {
                                'name': header_text,
                                'page_start': page_num,
                                'page_end': page_num
                            }
                            current_section = header_text
                
                # Close last section
                if current_section and current_section in headers:
                    headers[current_section]['page_end'] = len(pdf.pages)
                
                if len(headers) > 3:  # Found reasonable structure
                    sections = []
                    for header, data in headers.items():
                        sections.append({
                            'name': data['name'],
                            'page_start': data['page_start'],
                            'page_end': data['page_end'],
                            'blacklisted': self._is_financial_statement(data['name']),
                            'category': self._categorize_section(data['name'])
                        })
                    
                    return self._build_structure_dict(sections, 'header_analysis', len(pdf.pages))
            
            return None
        
        except Exception as e:
            logger.debug(f"Header extraction failed: {e}")
            return None
    
    def _fallback_structure(self):
        """
        Fallback: mark all pages as unknown
        """
        with pdfplumber.open(self.pdf_path) as pdf:
            total_pages = len(pdf.pages)
        
        sections = [{
            'name': 'Full Document',
            'page_start': 1,
            'page_end': total_pages,
            'blacklisted': False,
            'category': 'unknown'
        }]
        
        return self._build_structure_dict(sections, 'full_scan', total_pages)
    
    def _is_financial_statement(self, title):
        """
        Check if section is a financial statement (to blacklist)
        """
        title_lower = title.lower()
        
        blacklist_keywords = [
            'balance sheet',
            'statement of profit',
            'profit and loss',
            'p&l',
            'cash flow',
            'standalone financial',
            'consolidated financial',
            'auditor',
            'independent auditor'
        ]
        
        return any(keyword in title_lower for keyword in blacklist_keywords)
    
    def _categorize_section(self, title):
        """
        Categorize section for prioritization
        """
        title_lower = title.lower()
        
        if any(kw in title_lower for kw in ['financial statement', 'balance sheet', 'profit', 'cash flow']):
            return 'financial_statement'
        
        if any(kw in title_lower for kw in ['management discussion', 'md&a', 'business review']):
            return 'alpha_rich'
        
        if any(kw in title_lower for kw in ['notes to accounts', 'notes', 'schedules']):
            return 'alpha_rich'
        
        if any(kw in title_lower for kw in ['director', 'governance', 'board']):
            return 'governance'
        
        return 'other'
    
    def _build_structure_dict(self, sections, method, total_pages):
        """
        Build final structure dictionary
        """
        # Identify blacklisted pages
        blacklisted_pages = []
        candidate_pages = []
        
        for section in sections:
            page_range = range(section['page_start'], section['page_end'] + 1)
            if section['blacklisted']:
                blacklisted_pages.extend(page_range)
            else:
                candidate_pages.extend(page_range)
        
        return {
            'document_id': self.document_id,
            'company': self.company,
            'report_year': self.report_year,
            'industry': self.metadata['industry'],
            'fiscal_year_end': self.metadata['fiscal_year_end'],
            'total_pages': total_pages,
            'structure_method': method,
            'extracted_at': datetime.now().isoformat(),
            'sections': sections,
            'blacklisted_pages': sorted(list(set(blacklisted_pages))),
            'candidate_pages': sorted(list(set(candidate_pages))),
            'statistics': {
                'total_sections': len(sections),
                'blacklisted_sections': sum(1 for s in sections if s['blacklisted']),
                'candidate_sections': sum(1 for s in sections if not s['blacklisted']),
                'blacklisted_page_count': len(set(blacklisted_pages)),
                'candidate_page_count': len(set(candidate_pages))
            }
        }

def main(pdf_path, output_dir="output/stage0_structure"):
    """
    Main execution function
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 0: STRUCTURE EXTRACTION")
    logger.info(f"=" * 60)
    
    extractor = StructureExtractor(pdf_path)
    structure = extractor.extract_structure()
    
    # Save output
    output_file = os.path.join(output_dir, f"{structure['document_id']}_structure.json")
    save_json(structure, output_file)
    
    logger.info(f"Structure saved to: {output_file}")
    logger.info(f"  Total pages: {structure['total_pages']}")
    logger.info(f"  Sections found: {structure['statistics']['total_sections']}")
    logger.info(f"  Blacklisted pages: {structure['statistics']['blacklisted_page_count']}")
    logger.info(f"  Candidate pages: {structure['statistics']['candidate_page_count']}")
    
    return structure

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract PDF structure')
    parser.add_argument('pdf_path', help='Path to PDF file')
    parser.add_argument('--output-dir', default='output/stage0_structure', help='Output directory')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.pdf_path, args.output_dir)