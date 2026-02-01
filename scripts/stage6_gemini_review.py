"""
Stage 6: Gemini Review (for flagged items only)
Send low-confidence extractions to Gemini for final validation
"""

import os
import sys
import logging
import json
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import (
    setup_logging, load_json, save_json, create_output_directories,
    load_settings
)

logger = logging.getLogger(__name__)

class GeminiReviewer:
    """
    Review flagged extractions using Gemini
    """
    
    def __init__(self, verified_path, images_manifest_path, confidence_threshold=0.70):
        self.data = load_json(verified_path)
        self.manifest = load_json(images_manifest_path)
        self.settings = load_settings()
        
        self.document_id = self.data['document_id']
        self.extractions = self.data['extractions']
        self.confidence_threshold = confidence_threshold
        
        self.images_dir = self.manifest['output_directory']
        
        # Identify items needing review
        self.needs_review = self._identify_review_items()
    
    def _identify_review_items(self):
        """
        Identify extractions needing Gemini review
        """
        needs_review = []
        
        for extraction in self.extractions:
            # Criteria for review:
            # 1. Explicitly flagged
            # 2. Low confidence after verification
            # 3. Verification status is FLAGGED
            
            if extraction.get('flags', {}).get('needs_review', False):
                needs_review.append(extraction)
            elif extraction.get('verification_status') == 'FLAGGED':
                needs_review.append(extraction)
            elif extraction.get('confidence', 1.0) < self.confidence_threshold:
                needs_review.append(extraction)
        
        return needs_review
    
    def review_all(self):
        """
        Review all flagged items
        """
        if not self.needs_review:
            logger.info("No items need Gemini review - skipping")
            return self.extractions, {'total_reviewed': 0}
        
        logger.info(f"Reviewing {len(self.needs_review)} flagged items with Gemini...")
        
        reviewed_map = {}
        review_stats = {
            'total_reviewed': len(self.needs_review),
            'confirmed': 0,
            'corrected': 0,
            'still_ambiguous': 0
        }
        
        for i, extraction in enumerate(self.needs_review):
            logger.info(f"Reviewing {i+1}/{len(self.needs_review)}: {extraction.get('kpi_name')}...")
            
            try:
                review = self._review_extraction(extraction)
                reviewed_map[extraction['extraction_id']] = review
                
                decision = review.get('review_decision', 'AMBIGUOUS')
                if decision == 'CORRECT':
                    review_stats['confirmed'] += 1
                elif decision == 'INCORRECT':
                    review_stats['corrected'] += 1
                else:
                    review_stats['still_ambiguous'] += 1
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error reviewing extraction: {e}")
                continue
        
        # Apply reviews
        final_extractions = self._apply_reviews(self.extractions, reviewed_map)
        
        logger.info(f"Gemini review complete:")
        logger.info(f"  Confirmed: {review_stats['confirmed']}")
        logger.info(f"  Corrected: {review_stats['corrected']}")
        logger.info(f"  Still ambiguous: {review_stats['still_ambiguous']}")
        
        return final_extractions, review_stats
    
    def _review_extraction(self, extraction):
        """
        Review a single extraction with Gemini
        """
        page_num = extraction.get('source', {}).get('page', 0)
        image_path = os.path.join(self.images_dir, f"page_{page_num:03d}.png")
        
        if not os.path.exists(image_path):
            logger.warning(f"Image not found for review: {image_path}")
            return {'review_decision': 'AMBIGUOUS'}
        
        # Build prompt
        prompt = self._build_review_prompt(extraction)
        
        # Call Gemini
        result = self._call_gemini(image_path, prompt)
        
        return result
    
    def _build_review_prompt(self, extraction):
        """
        Build review prompt for Gemini
        """
        prompt = f"""You are a financial analyst reviewing AI-extracted data.

CONTEXT:
An AI extracted this KPI but flagged it for review due to uncertainty.

EXTRACTION:
{{
  "kpi_name": "{extraction.get('kpi_name')}",
  "value": {extraction.get('value_numeric')},
  "unit": "{extraction.get('unit')}",
  "fiscal_year": {extraction.get('fiscal_year')},
  "confidence": {extraction.get('confidence')},
  "issue": "{extraction.get('verification_notes', 'Low confidence')}"
}}

PAGE IMAGE:
[Image attached]

TASK:
1. Review the page image
2. Determine if the extraction is:
   - CORRECT (confirm value + reasoning)
   - INCORRECT (provide corrected value + reasoning)
   - AMBIGUOUS (explain why it cannot be determined)

3. If ambiguous, suggest what additional context would help

OUTPUT JSON ONLY:
{{
  "review_decision": "CORRECT" | "INCORRECT" | "AMBIGUOUS",
  "corrected_value": null | {{value}},
  "gemini_confidence": 0.0-1.0,
  "reasoning": "...",
  "additional_context_needed": "..." (if ambiguous)
}}
"""
        
        return prompt
    
    def _call_gemini(self, image_path, prompt):
        """
        Call Gemini for review
        """
        try:
            import google.generativeai as genai
            
            genai.configure(api_key=self.settings['gemini_api_key'])
            
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            response = model.generate_content([
                prompt,
                {'mime_type': 'image/png', 'data': image_data}
            ])
            
            response_text = response.text
            
            # Parse JSON
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0].strip()
            
            result = json.loads(response_text)
            return result
            
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return {'review_decision': 'AMBIGUOUS', 'reasoning': str(e)}
    
    def _apply_reviews(self, extractions, reviewed_map):
        """
        Apply Gemini reviews to extractions
        """
        final = []
        
        for extraction in extractions:
            ext_id = extraction.get('extraction_id')
            
            if ext_id in reviewed_map:
                review = reviewed_map[ext_id]
                decision = review.get('review_decision', 'AMBIGUOUS')
                
                if decision == 'CORRECT':
                    extraction['gemini_reviewed'] = True
                    extraction['gemini_decision'] = 'CONFIRMED'
                    extraction['confidence'] = review.get('gemini_confidence', 0.95)
                    extraction['flags']['needs_review'] = False
                    extraction['gemini_notes'] = review.get('reasoning', 'Confirmed by Gemini')
                
                elif decision == 'INCORRECT':
                    extraction['gemini_reviewed'] = True
                    extraction['gemini_decision'] = 'CORRECTED'
                    extraction['value_numeric'] = review.get('corrected_value')
                    extraction['value_actual'] = review.get('corrected_value')
                    extraction['confidence'] = review.get('gemini_confidence', 0.90)
                    extraction['flags']['needs_review'] = False
                    extraction['gemini_notes'] = f"Corrected by Gemini: {review.get('reasoning', '')}"
                
                else:  # AMBIGUOUS
                    extraction['gemini_reviewed'] = True
                    extraction['gemini_decision'] = 'AMBIGUOUS'
                    extraction['flags']['needs_review'] = True
                    extraction['review_tier'] = 'manual'
                    extraction['gemini_notes'] = f"Could not resolve: {review.get('reasoning', '')}"
            
            final.append(extraction)
        
        return final

def main(verified_path, images_manifest_path, output_dir="output/stage6_gemini_reviewed", 
         confidence_threshold=0.70):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 6: GEMINI REVIEW")
    logger.info(f"=" * 60)
    
    reviewer = GeminiReviewer(verified_path, images_manifest_path, confidence_threshold)
    
    logger.info(f"Items needing review: {len(reviewer.needs_review)}")
    
    if len(reviewer.needs_review) == 0:
        logger.info("✓ No items need review - using verified data as-is")
        # Just copy verified to final
        output = reviewer.data
        output['gemini_review_stats'] = {'total_reviewed': 0}
    else:
        final_extractions, stats = reviewer.review_all()
        
        output = {
            'document_id': reviewer.document_id,
            'total_extractions': len(final_extractions),
            'gemini_review_stats': stats,
            'extractions': final_extractions
        }
    
    # Save
    output_file = os.path.join(output_dir, f"{reviewer.document_id}_gemini_reviewed.json")
    save_json(output, output_file)
    
    logger.info(f"✓ Final data saved: {output_file}")
    
    # Count final needs_review
    still_needs_review = sum(1 for e in output['extractions'] 
                            if e.get('flags', {}).get('needs_review', False))
    logger.info(f"  Still needs manual review: {still_needs_review}")
    
    return output

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Gemini review of flagged items')
    parser.add_argument('verified_path', help='Path to verified JSON from Stage 5')
    parser.add_argument('manifest_path', help='Path to image manifest from Stage 2')
    parser.add_argument('--output-dir', default='output/stage6_gemini_reviewed')
    parser.add_argument('--threshold', type=float, default=0.70, 
                       help='Confidence threshold for Gemini review')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.verified_path, args.manifest_path, args.output_dir, args.threshold)
