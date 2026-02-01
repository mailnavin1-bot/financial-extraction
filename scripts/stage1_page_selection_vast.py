"""
Stage 1: Page Scoring and Selection using Llama 3.2 3B on Vast.ai
Scores pages by 'alpha density' and validates with LLM
"""

import os
import sys
import logging
import re
import requests
import json
from collections import Counter
import pdfplumber

# Add parent directory to path to allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import setup_logging, load_json, save_json, create_output_directories, load_settings
from vast.vast_manager import VastManager

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
        
        # Settings
        self.settings = load_settings()
        mode = self.settings.get('mode', 'pilot')
        
        if mode == 'pilot':
            self.config = self.settings['pilot_config']
        else:
            self.config = self.settings['production_config']
        
        # Vast manager
        self.vast = VastManager(use_spot=self.config['use_spot_instances'])
        self.instance_info = None
        
    def score_pages(self):
        """
        Score all candidate pages using keyword-based heuristics
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
        Select top pages for LLM validation
        """
        # Filter by minimum score
        filtered = [p for p in page_scores if p['score'] >= min_score]
        
        # Take top N
        selected = filtered[:max_pages]
        
        logger.info(f"Selected {len(selected)} pages for LLM validation (score >= {min_score})")
        
        return selected
    
    def validate_with_llm(self, selected_pages):
        """
        Validate selected pages using Llama 3.2 3B on Vast.ai
        """
        logger.info("Launching Vast.ai instance for LLM validation...")
        
        try:
            # Launch instance
            self.instance_info = self.vast.launch_for_stage1(
                max_price=self.config['max_price_stage1']
            )
            
            logger.info(f"Instance ready: {self.instance_info['api_url']}")
            logger.info(f"Cost: ${self.instance_info['price_per_hour']:.3f}/hr")
            
            # Extract text from selected pages
            page_texts = self._extract_page_texts(selected_pages)
            
            # Send to LLM for validation
            validated = self._call_llm_validation(page_texts)
            
            return validated
            
        finally:
            # Always destroy instance
            if self.instance_info:
                self.vast.destroy_instance(self.instance_info['instance_id'])
    
    def _extract_page_texts(self, selected_pages):
        """
        Extract text from selected pages
        """
        logger.info("Extracting text from selected pages...")
        
        page_texts = {}
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_info in selected_pages:
                page_num = page_info['page']
                page = pdf.pages[page_num - 1]
                text = page.extract_text()
                
                if text:
                    page_texts[page_num] = text[:2000]  # First 2000 chars
        
        return page_texts
    
    def _call_llm_validation(self, page_texts):
        """
        Call Llama 3.2 3B to validate pages
        """
        logger.info(f"Validating {len(page_texts)} pages with Llama 3.2 3B...")
        
        # Build validation prompt
        prompt = self._build_validation_prompt(page_texts)
        
        # Call API
        try:
            response = requests.post(
                f"{self.instance_info['api_url']}/validate",
                json={'page_texts': page_texts, 'prompt': prompt},
                timeout=300
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Filter pages based on LLM response
                validated_pages = []
                for page_num, validation in result['validations'].items():
                    if validation['has_operational_kpis'] or validation['has_disclosures']:
                        if not validation['is_financial_statement']:
                            if validation['confidence'] > 0.7:
                                validated_pages.append(int(page_num))
                
                logger.info(f"LLM validated {len(validated_pages)} pages")
                
                return validated_pages
            else:
                logger.error(f"LLM validation failed: {response.text}")
                # Fallback: return all selected pages
                return list(page_texts.keys())
                
        except Exception as e:
            logger.error(f"LLM validation error: {e}")
            # Fallback: return all selected pages
            return list(page_texts.keys())
    
    def _build_validation_prompt(self, page_texts):
        """
        Build prompt for LLM validation
        """
        prompt = f"""You are reviewing pages from {self.structure['company']} Annual Report {self.structure['report_year']}.

I have identified {len(page_texts)} pages that MIGHT contain operational KPIs or material disclosures.

For each page, determine:
1. Does it contain operational metrics? (YES/NO)
2. Does it contain material disclosures? (YES/NO)
3. Is it a financial statement table? (YES/NO)
4. Confidence (0-1)

PAGES:
{json.dumps({str(k): v for k, v in page_texts.items()}, indent=2)}

OUTPUT JSON:
{{
  "validations": {{
    "page_num": {{
      "has_operational_kpis": true/false,
      "has_disclosures": true/false,
      "is_financial_statement": true/false,
      "confidence": 0.0-1.0,
      "reasoning": "..."
    }}
  }}
}}
"""
        
        return prompt

def main(structure_json_path, pdf_path, output_dir="output/stage1_flagged_pages"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 1: PAGE SELECTION (VAST.AI)")
    logger.info(f"=" * 60)
    
    selector = PageSelector(structure_json_path, pdf_path)
    
    # Step 1: Score all pages (local, keyword-based)
    page_scores = selector.score_pages()
    
    # Step 2: Select top pages
    selected_pages = selector.select_top_pages(page_scores, max_pages=80, min_score=0.5)
    
    # Step 3: Validate with LLM (Vast.ai)
    validated_page_nums = selector.validate_with_llm(selected_pages)
    
    # Build final output
    final_pages = [p for p in selected_pages if p['page'] in validated_page_nums]
    
    output = {
        'document_id': selector.document_id,
        'total_pages_analyzed': len(selector.candidate_pages),
        'pages_scored': len(page_scores),
        'pages_sent_to_llm': len(selected_pages),
        'pages_selected': len(final_pages),
        'flagged_pages': [p['page'] for p in final_pages],
        'page_details': final_pages,
        'statistics': {
            'avg_score': round(sum(p['score'] for p in page_scores) / len(page_scores), 2) if page_scores else 0,
            'max_score': max(p['score'] for p in page_scores) if page_scores else 0,
            'min_score_selected': min(p['score'] for p in final_pages) if final_pages else 0
        }
    }
    
    # Save
    output_file = os.path.join(output_dir, f"{selector.document_id}_flagged.json")
    save_json(output, output_file)
    
    logger.info(f"Flagged pages saved to: {output_file}")
    logger.info(f"  Final pages selected: {len(final_pages)}")
    
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