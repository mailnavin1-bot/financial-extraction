"""
Stage 3: KPI Extraction using Vision LLM
Uses Modal/RunPod serverless for Qwen2.5-VL-72B inference
"""

import os
import sys
import logging
import json
import base64
from datetime import datetime
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import (
    setup_logging, load_json, save_json, create_output_directories,
    load_prompt_template, generate_extraction_id, load_settings
)

logger = logging.getLogger(__name__)

class KPIExtractor:
    """
    Extract KPIs using vision LLM via serverless
    """
    
    def __init__(self, manifest_path, structure_path):
        self.manifest = load_json(manifest_path)
        self.structure = load_json(structure_path)
        self.settings = load_settings()
        
        self.document_id = self.manifest['document_id']
        self.images = self.manifest['images']
        
        # Load prompt template
        self.prompt_template = load_prompt_template()
        
        # Output directory
        self.output_dir = os.path.join("output/stage3_extractions", self.document_id)
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract_all_pages(self, use_modal=True):
        """
        Extract KPIs from all images
        
        For pilot: using API-based approach (Modal/RunPod serverless)
        For production: will use Vast.ai with custom Docker
        """
        logger.info(f"Extracting KPIs from {len(self.images)} pages...")
        
        extractions = []
        
        for i, img_info in enumerate(self.images):
            logger.info(f"Processing page {img_info['page']} ({i+1}/{len(self.images)})...")
            
            try:
                extraction = self._extract_page(img_info)
                
                # Save individual extraction
                output_file = os.path.join(
                    self.output_dir, 
                    f"page_{img_info['page']:03d}_extraction.json"
                )
                save_json(extraction, output_file)
                
                extractions.append(extraction)
                
                logger.info(f"  ✓ Extracted {len(extraction.get('extractions', []))} KPIs")
                
                # Rate limiting for API
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  ✗ Error extracting page {img_info['page']}: {e}")
                continue
        
        return extractions
    
    def _extract_page(self, img_info):
        """
        Extract KPIs from a single page image
        """
        # Build prompt
        prompt = self._build_prompt(img_info)
        
        # For pilot: Use Gemini Vision API (easier setup)
        # For production: Switch to Qwen2.5-VL-72B on Vast.ai
        
        if self.settings.get('use_gemini_for_extraction', True):
            result = self._call_gemini_vision(img_info['filepath'], prompt)
        else:
            # Placeholder for Vast.ai/Modal implementation
            result = self._call_modal_inference(img_info['filepath'], prompt)
        
        return result
    
    def _build_prompt(self, img_info):
        """
        Build extraction prompt for a specific page
        """
        page_num = img_info['page']
        
        # Get section name for this page
        section_name = "Unknown"
        for section in self.structure['sections']:
            if section['page_start'] <= page_num <= section['page_end']:
                section_name = section['name']
                break
        
        # Fill in prompt template
        prompt = self.prompt_template.format(
            company_name=self.structure['company'],
            industry_name=self.structure['industry'],
            report_year=self.structure['report_year'],
            fiscal_year_end=self.structure['fiscal_year_end'],
            page_number=page_num,
            section_name=section_name
        )
        
        return prompt
    
    def _call_gemini_vision(self, image_path, prompt):
        """
        Call Gemini Vision API for extraction
        """
        try:
            import google.generativeai as genai
            
            genai.configure(api_key=self.settings['gemini_api_key'])
            
            # Load image
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            # Create model
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            # Generate content
            response = model.generate_content([
                prompt,
                {'mime_type': 'image/png', 'data': image_data}
            ])
            
            # Parse JSON response
            response_text = response.text
            
            # Extract JSON from markdown code blocks if present
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0].strip()
            
            result = json.loads(response_text)
            
            # Add extraction IDs
            if 'extractions' in result:
                for extraction in result['extractions']:
                    if 'extraction_id' not in extraction:
                        extraction['extraction_id'] = generate_extraction_id()
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Response text: {response_text}")
            
            # Return empty extraction
            return {
                'page_metadata': {
                    'page_number': img_info['page'],
                    'error': 'JSON parse error'
                },
                'extractions': []
            }
        
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise
    
    def _call_modal_inference(self, image_path, prompt):
        """
        Placeholder for Modal/Vast.ai inference
        To be implemented for production
        """
        raise NotImplementedError("Modal/Vast.ai inference not yet implemented. Use Gemini for pilot.")

def main(manifest_path, structure_path, output_dir="output/stage3_extractions"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 3: KPI EXTRACTION")
    logger.info(f"=" * 60)
    
    extractor = KPIExtractor(manifest_path, structure_path)
    
    # Extract all pages
    extractions = extractor.extract_all_pages()
    
    logger.info(f"✓ Extraction complete")
    logger.info(f"  Pages processed: {len(extractions)}")
    logger.info(f"  Output directory: {extractor.output_dir}")
    
    # Summary stats
    total_kpis = sum(len(e.get('extractions', [])) for e in extractions)
    logger.info(f"  Total KPIs extracted: {total_kpis}")
    
    return extractions

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract KPIs using vision LLM')
    parser.add_argument('manifest_path', help='Path to image manifest from Stage 2')
    parser.add_argument('structure_path', help='Path to structure JSON from Stage 0')
    parser.add_argument('--output-dir', default='output/stage3_extractions')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.manifest_path, args.structure_path, args.output_dir)
