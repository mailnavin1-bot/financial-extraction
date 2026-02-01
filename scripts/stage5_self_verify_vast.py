"""
Stage 5: Self-Verification using Qwen2.5-VL-72B on Vast.ai
Vision LLM reviews its own extractions for errors
"""

import os
import sys
import logging
import json
import requests
import time

# Add parent directory to path to allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import (
    setup_logging, load_json, save_json, create_output_directories,
    load_settings
)
from vast.vast_manager import VastManager

logger = logging.getLogger(__name__)

class VastSelfVerifier:
    """
    Self-verify extractions using Qwen2.5-VL-72B on Vast.ai
    """
    
    def __init__(self, filtered_path, images_manifest_path):
        self.data = load_json(filtered_path)
        self.manifest = load_json(images_manifest_path)
        
        self.document_id = self.data['document_id']
        self.extractions = self.data['extractions']
        
        # Group extractions by source page
        self.extractions_by_page = self._group_by_page()
        
        # Get image paths
        self.images_dir = self.manifest['output_directory']
        
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
    
    def _group_by_page(self):
        """
        Group extractions by source page
        """
        from collections import defaultdict
        grouped = defaultdict(list)
        
        for extraction in self.extractions:
            page = extraction.get('source', {}).get('page', 0)
            grouped[page].append(extraction)
        
        return grouped
    
    def verify_all(self):
        """
        Verify all extractions using Vast.ai
        """
        logger.info(f"Verifying extractions from {len(self.extractions_by_page)} pages...")
        
        try:
            # Launch instance
            self._launch_instance()
            
            # Process pages
            verified_extractions, stats = self._process_all_pages()
            
            logger.info(f"Verification complete")
            logger.info(f"  Confirmed: {stats['confirmed']}")
            logger.info(f"  Corrected: {stats['corrected']}")
            logger.info(f"  Flagged: {stats['flagged']}")
            
            return verified_extractions, stats
            
        finally:
            # Always destroy instance
            self._destroy_instance()
    
    def _launch_instance(self):
        """
        Launch Vast.ai instance for verification
        """
        logger.info("Launching Vast.ai instance for verification...")
        
        self.instance_info = self.vast.launch_for_stage5(
            max_price=self.config['max_price_stage5']
        )
        
        logger.info(f"Instance ready: {self.instance_info['api_url']}")
        logger.info(f"  Cost: ${self.instance_info['price_per_hour']:.3f}/hr")
    
    def _process_all_pages(self):
        """
        Process all pages for verification
        """
        verified_extractions = []
        stats = {
            'confirmed': 0,
            'corrected': 0,
            'flagged': 0
        }
        
        page_count = 0
        total_pages = len(self.extractions_by_page)
        
        for page_num, page_extractions in self.extractions_by_page.items():
            page_count += 1
            logger.info(f"Verifying page {page_num} ({page_count}/{total_pages})...")
            
            try:
                verified = self._verify_page(page_num, page_extractions)
                
                # Update stats
                for v in verified:
                    status = v.get('verification_status', 'unknown')
                    if status == 'CONFIRMED':
                        stats['confirmed'] += 1
                    elif status == 'CORRECTED':
                        stats['corrected'] += 1
                    elif status == 'FLAGGED':
                        stats['flagged'] += 1
                
                verified_extractions.extend(verified)
                
            except Exception as e:
                logger.error(f"  Error verifying page {page_num}: {e}")
                # Keep original extractions
                verified_extractions.extend(page_extractions)
        
        return verified_extractions, stats
    
    def _verify_page(self, page_num, extractions):
        """
        Verify extractions from a single page
        """
        # Get image path
        image_path = os.path.join(self.images_dir, f"page_{page_num:03d}.png")
        
        if not os.path.exists(image_path):
            logger.warning(f"  Image not found: {image_path}")
            return extractions
        
        # Build verification prompt
        prompt = self._build_verification_prompt(page_num, extractions)
        
        # Simplify extractions for API
        simplified_extractions = []
        for e in extractions:
            simplified_extractions.append({
                'extraction_id': e.get('extraction_id'),
                'kpi_name': e.get('kpi_name'),
                'fiscal_year': e.get('fiscal_year'),
                'value': e.get('value_numeric'),
                'unit': e.get('unit'),
                'confidence': e.get('confidence')
            })
        
        # Call Vast.ai API
        try:
            with open(image_path, 'rb') as f:
                response = requests.post(
                    f"{self.instance_info['api_url']}/verify",
                    files={'image': (f"page_{page_num}.png", f, 'image/png')},
                    data={
                        'prompt': prompt,
                        'extractions': json.dumps(simplified_extractions)
                    },
                    timeout=300
                )
            
            if response.status_code == 200:
                verification_result = response.json()
                
                # Apply corrections
                verified = self._apply_corrections(extractions, verification_result)
                
                return verified
            else:
                logger.error(f"  API error: {response.status_code}")
                return extractions
                
        except Exception as e:
            logger.error(f"  Verification API call failed: {e}")
            return extractions
    
    def _build_verification_prompt(self, page_num, extractions):
        """
        Build verification prompt
        """
        prompt = f"""You previously extracted these KPIs from page {page_num}.

TASK: Review your own extractions and check for errors.

Common errors to check:
1. Wrong fiscal year column selected
2. Wrong row (extracted wrong metric)
3. Misread number (OCR error)
4. Wrong units (thousands vs millions vs crores)
5. Missed footnote that changes interpretation
6. Extracted financial statement item by mistake

OUTPUT JSON:
{{
  "page": {page_num},
  "review_status": "CONFIRMED" | "CORRECTED" | "FLAGGED",
  "corrections": [
    {{
      "extraction_id": "uuid",
      "kpi_name": "...",
      "original_value": ...,
      "corrected_value": ... | null,
      "status": "CONFIRMED" | "CORRECTED" | "FLAGGED",
      "verification_confidence": 0.0-1.0,
      "reasoning": "..."
    }}
  ]
}}
"""
        
        return prompt
    
    def _apply_corrections(self, extractions, verification_result):
        """
        Apply verification corrections to extractions
        """
        corrections_map = {}
        for correction in verification_result.get('corrections', []):
            ext_id = correction.get('extraction_id')
            corrections_map[ext_id] = correction
        
        verified = []
        
        for extraction in extractions:
            ext_id = extraction.get('extraction_id')
            
            if ext_id in corrections_map:
                correction = corrections_map[ext_id]
                status = correction.get('status', 'CONFIRMED')
                
                if status == 'CONFIRMED':
                    # Boost confidence slightly
                    extraction['confidence'] = min(extraction['confidence'] * 1.05, 1.0)
                    extraction['verification_status'] = 'CONFIRMED'
                    extraction['verification_confidence'] = correction.get('verification_confidence', 1.0)
                    extraction['verification_notes'] = correction.get('reasoning', 'Verified in self-review')
                
                elif status == 'CORRECTED':
                    # Apply correction
                    original_value = extraction.get('value_numeric')
                    corrected_value = correction.get('corrected_value')
                    
                    extraction['value_numeric'] = corrected_value
                    extraction['value_actual'] = corrected_value
                    extraction['confidence'] = correction.get('verification_confidence', 0.9)
                    extraction['verification_status'] = 'CORRECTED'
                    extraction['verification_confidence'] = correction.get('verification_confidence', 0.9)
                    extraction['verification_notes'] = f"Corrected from {original_value} to {corrected_value}: {correction.get('reasoning', '')}"
                
                elif status == 'FLAGGED':
                    # Flag for review
                    extraction['verification_status'] = 'FLAGGED'
                    extraction['verification_confidence'] = correction.get('verification_confidence', 0.5)
                    extraction['verification_notes'] = correction.get('reasoning', 'Flagged in self-review')
                    extraction['flags'] = extraction.get('flags', {})
                    extraction['flags']['needs_review'] = True
            else:
                # No correction found, mark as confirmed
                extraction['verification_status'] = 'CONFIRMED'
                extraction['verification_confidence'] = extraction.get('confidence', 0.9)
            
            verified.append(extraction)
        
        return verified
    
    def _destroy_instance(self):
        """
        Destroy Vast.ai instance
        """
        if self.instance_info:
            logger.info("Shutting down Vast.ai instance...")
            self.vast.destroy_instance(self.instance_info['instance_id'])
            logger.info("Instance destroyed")

def main(filtered_path, images_manifest_path, output_dir="output/stage5_verified"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 5: SELF-VERIFICATION (VAST.AI)")
    logger.info(f"=" * 60)
    
    verifier = VastSelfVerifier(filtered_path, images_manifest_path)
    verified_extractions, stats = verifier.verify_all()
    
    # Build output
    output = {
        'document_id': verifier.document_id,
        'total_extractions': len(verified_extractions),
        'verification_stats': stats,
        'extractions': verified_extractions
    }
    
    # Save
    output_file = os.path.join(output_dir, f"{verifier.document_id}_verified.json")
    save_json(output, output_file)
    
    logger.info(f"Verified data saved: {output_file}")
    logger.info(f"  Correction rate: {stats['corrected']/len(verified_extractions)*100:.1f}%")
    
    return output

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Self-verify extractions on Vast.ai')
    parser.add_argument('filtered_path', help='Path to filtered JSON from Stage 4.5')
    parser.add_argument('manifest_path', help='Path to image manifest from Stage 2')
    parser.add_argument('--output-dir', default='output/stage5_verified')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.filtered_path, args.manifest_path, args.output_dir)