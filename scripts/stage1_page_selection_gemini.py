"""
Stage 1: Page Scoring and Selection
Score pages by 'alpha density' and select top candidates for extraction
"""

import os
import sys
import logging
import re
from collections import Counter
import pdfplumber

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, save_json, create_output_directories

logger = logging.getLogger(__name__)

class PageSelector:
    """
    Score and select pages for KPI extraction
    """
    
    # Keywords for different categories (industry-agnostic)
    KEYWORDS = {
        'employee_metrics': [
            'employee', 'headcount', 'attrition', 'workforce', 'staff',
            'personnel', 'turnover', 'retention', 'utilization'
        ],
        'operational_scale': [
            'branch', 'store', 'office', 'factory', 'plant', 'facility',
            'customer', 'subscriber', 'user', 'account', 'capacity'
        ],
        'business_mix': [
            'geography', 'segment', 'vertical', 'product line',
            'revenue by', 'breakdown', 'distribution'
        ],
        'disclosures': [
            'contingent liabilit', 'related party', 'litigation',
            'commitment', 'guarantee', 'pledge', 'subsequent event'
        ],
        'forward_looking': [
            'capex', 'capital expenditure', 'expansion', 'guidance',
            'outlook', 'order book', 'pipeline', 'backlog'
        ]
    }
    
    def __init__(self, structure_json_path, pdf_path):
        self.structure = load_json(structure_json_path)
        self.pdf_path = pdf_path
        self.document_id = self.structure['document_id']
        self.candidate_pages = self.structure['candidate_pages']
        
    def score_pages(self):
        """
        Score all candidate pages
        """
        logger.info(f"Scoring {len(self.candidate_pages)} candidate pages...")
        
        page_scores = []
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num in self.candidate_pages:
                page = pdf.pages[page_num - 1]
                text = page.extract_text()
                
                if not text:
                    continue
                
                score = self._calculate_page_score(page_num, text, page)
                
                page_scores.append({
                    'page': page_num,
                    'score': score['total'],
                    'breakdown': score,
                    'section': self._get_section_name(page_num)
                })
        
        # Sort by score
        page_scores.sort(key=lambda x: x['score'], reverse=True)
        
        return page_scores
    
    def _calculate_page_score(self, page_num, text, page):
        """
        Calculate comprehensive score for a page
        """
        text_lower = text.lower()
        score = 0.0
        breakdown = {}
        
        # 1. Section-based scoring
        section = self._get_section_name(page_num)
        section_score = self._score_section(section)
        score += section_score
        breakdown['section'] = section_score
        
        # 2. Keyword presence
        keyword_score = 0
        for category, keywords in self.KEYWORDS.items():
            category_matches = sum(1 for kw in keywords if kw in text_lower)
            if category_matches > 0:
                keyword_score += category_matches * 0.5
        score += keyword_score
        breakdown['keywords'] = keyword_score
        
        # 3. Table presence
        tables = page.extract_tables()
        table_score = min(len(tables) * 0.8, 3.0)  # Cap at 3.0
        score += table_score
        breakdown['tables'] = table_score
        
        # 4. Multi-year indicator
        if self._has_multi_year_columns(text):
            score += 1.5
            breakdown['multi_year'] = 1.5
        else:
            breakdown['multi_year'] = 0
        
        # 5. Number density
        numbers = re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', text)
        number_score = min(len(numbers) * 0.1, 2.0)
        score += number_score
        breakdown['numbers'] = number_score
        
        breakdown['total'] = round(score, 2)
        
        return breakdown
    
    def _score_section(self, section_name):
        """
        Score based on section category
        """
        section_lower = section_name.lower()
        
        if any(kw in section_lower for kw in ['management discussion', 'md&a', 'business review']):
            return 2.0
        
        if any(kw in section_lower for kw in ['notes to accounts', 'notes', 'schedules']):
            return 1.5
        
        if any(kw in section_lower for kw in ['director', 'corporate governance']):
            return 1.0
        
        return 0.5
    
    def _has_multi_year_columns(self, text):
        """
        Detect if text contains multi-year column headers
        """
        # Pattern: FY24, FY23, FY22 or similar
        pattern = r'FY\s*2[0-9]{1,2}\s+FY\s*2[0-9]{1,2}'
        return bool(re.search(pattern, text))
    
    def _get_section_name(self, page_num):
        """
        Get section name for a page number
        """
        for section in self.structure['sections']:
            if section['page_start'] <= page_num <= section['page_end']:
                return section['name']
        return 'Unknown'
    
    def select_top_pages(self, page_scores, max_pages=80, min_score=0.5):
        """
        Select top pages for processing
        """
        # Filter by minimum score
        filtered = [p for p in page_scores if p['score'] >= min_score]
        
        # Take top N
        selected = filtered[:max_pages]
        
        logger.info(f"Selected {len(selected)} pages (score ≥ {min_score})")
        
        return selected

def main(structure_json_path, pdf_path, output_dir="output/stage1_flagged_pages"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 1: PAGE SELECTION")
    logger.info(f"=" * 60)
    
    selector = PageSelector(structure_json_path, pdf_path)
    
    # Score all pages
    page_scores = selector.score_pages()
    
    # Select top pages
    selected_pages = selector.select_top_pages(page_scores, max_pages=80, min_score=0.5)
    
    # Build output
    output = {
        'document_id': selector.document_id,
        'total_pages_analyzed': len(selector.candidate_pages),
        'pages_scored': len(page_scores),
        'pages_selected': len(selected_pages),
        'flagged_pages': [p['page'] for p in selected_pages],
        'page_details': selected_pages,
        'statistics': {
            'avg_score': round(sum(p['score'] for p in page_scores) / len(page_scores), 2) if page_scores else 0,
            'max_score': max(p['score'] for p in page_scores) if page_scores else 0,
            'min_score_selected': min(p['score'] for p in selected_pages) if selected_pages else 0
        }
    }
    
    # Save
    output_file = os.path.join(output_dir, f"{selector.document_id}_flagged.json")
    save_json(output, output_file)
    
    logger.info(f"✓ Flagged pages saved to: {output_file}")
    logger.info(f"  Pages selected: {len(selected_pages)}")
    logger.info(f"  Avg score: {output['statistics']['avg_score']}")
    
    return output

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Select pages for extraction')
    parser.add_argument('structure_json', help='Path to structure JSON from Stage 0')
    parser.add_argument('pdf_path', help='Path to PDF file')
    parser.add_argument('--output-dir', default='output/stage1_flagged_pages')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.structure_json, args.pdf_path, args.output_dir)
