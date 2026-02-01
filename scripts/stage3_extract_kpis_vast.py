"""
Stage 3: KPI Extraction using Qwen2.5-VL-72B on Vast.ai
Extract operational KPIs from flagged pages using vision LLM
"""

import os
import sys
import logging
import json
import requests
import time
from pathlib import Path

# Add parent directory to path to allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import (
    setup_logging, load_json, save_json, create_output_directories,
    load_prompt_template, generate_extraction_id, load_settings
)
from vast.vast_manager import VastManager

logger = logging.getLogger(__name__)

class VastKPIExtractor:
    """
    Extract KPIs using Qwen2.5-VL-72B on Vast.ai
    """
    
    def __init__(self, manifest_path, structure_path):
        self.manifest = load_json(manifest_path)
        self.structure = load_json(structure_path)
        
        self.document_id = self.manifest['document_id']
        self.images = self.manifest['images']
        self.images_dir = self.manifest['output_directory']
        
        # Load prompt template
        self.prompt_template = load_prompt_template()
        
        # Output directory
        self.output_dir = os.path.join("output/stage3_extractions", self.document_id)
        os.makedirs(self.output_dir, exist_ok=True)
        
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
    
    def extract_all_pages(self):
        """
        Extract KPIs from all images using Vast.ai
        """
        logger.info(f"Extracting KPIs from {len(self.images)} pages...")
        
        try:
            # Launch Vast.ai instance
            self._launch_instance()
            
            # Process in batches
            extractions = self._process_in_batches()
            
            logger.info(f"Extraction complete")
            logger.info(f"  Pages processed: {len(extractions)}")
            logger.info(f"  Total KPIs: {sum(len(e.get('extractions', [])) for e in extractions)}")
            
            return extractions
            
        finally:
            # Always destroy instance
            self._destroy_instance()
    
    def _launch_instance(self):
        """
        Launch Vast.ai instance for extraction
        """
        logger.info("Launching Vast.ai instance for extraction...")
        
        self.instance_info = self.vast.launch_for_stage3(
            max_price=self.config['max_price_stage3']
        )
        
        logger.info(f"Instance ready: {self.instance_info['api_url']}")
        logger.info(f"  GPU: {self.instance_info['gpu_count']}x {self.instance_info['gpu_name']}")
        logger.info(f"  Cost: ${self.instance_info['price_per_hour']:.3f}/hr")
    
    def _process_in_batches(self):
        """
        Process images in batches of 20
        """
        batch_size = self.settings['processing_config'].get('batch_size_stage3', 20)
        
        all_extractions = []
        
        num_batches = (len(self.images) + batch_size - 1) // batch_size
        
        for i in range(0, len(self.images), batch_size):
            batch = self.images[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            logger.info(f"Processing batch {batch_num}/{num_batches} ({len(batch)} pages)...")
            
            batch_start = time.time()
            
            try:
                batch_results = self._extract_batch(batch)
                all_extractions.extend(batch_results)
                
                batch_time = time.time() - batch_start
                logger.info(f"  Batch completed in {batch_time:.1f}s")
                
            except Exception as e:
                logger.error(f"  Batch {batch_num} failed: {e}")
                # Continue with next batch
                continue
        
        return all_extractions
    
    def _extract_batch(self, batch):
        """
        Extract KPIs from a batch of images
        """
        # Prepare files and prompts
        files = []
        prompts = []
        
        for img_info in batch:
            image_path = img_info['filepath']
            prompt = self._build_prompt(img_info)
            
            with open(image_path, 'rb') as f:
                files.append(('images', (os.path.basename(image_path), f.read(), 'image/png')))
            
            prompts.append(prompt)
        
        # Call Vast.ai API
        try:
            response = requests.post(
                f"{self.instance_info['api_url']}/extract_batch",
                files=files,
                data={'prompts': json.dumps(prompts)},
                timeout=1200  # 20 min timeout for batch
            )
            
            if response.status_code == 200:
                result = response.json()
                batch_results = result['results']
                
                # Save individual extractions
                for i, extraction_result in enumerate(batch_results):
                    img_info = batch[i]
                    
                    # Add extraction IDs
                    if 'extractions' in extraction_result:
                        for extraction in extraction_result['extractions']:
                            if 'extraction_id' not in extraction:
                                extraction['extraction_id'] = generate_extraction_id()
                    
                    # Save to file
                    output_file = os.path.join(
                        self.output_dir,
                        f"page_{img_info['page']:03d}_extraction.json"
                    )
                    save_json(extraction_result, output_file)
                    
                    logger.debug(f"    Page {img_info['page']}: {len(extraction_result.get('extractions', []))} KPIs")
                
                return batch_results
            else:
                raise Exception(f"API error: {response.status_code} - {response.text}")
                
        except requests.exceptions.Timeout:
            raise Exception("Batch processing timeout (>20 min)")
        except Exception as e:
            raise Exception(f"Batch extraction failed: {str(e)}")
    
    def _build_prompt(self, img_info):
        """
        Build extraction prompt for a specific page
        """
        page_num = img_info['page']
        
        # Get section name
        section_name = "Unknown"
        for section in self.structure['sections']:
            if section['page_start'] <= page_num <= section['page_end']:
                section_name = section['name']
                break
        
        # Fill prompt template
        prompt = self.prompt_template.format(
            company_name=self.structure['company'],
            industry_name=self.structure['industry'],
            report_year=self.structure['report_year'],
            fiscal_year_end=self.structure['fiscal_year_end'],
            page_number=page_num,
            section_name=section_name
        )
        
        return prompt
    
    def _destroy_instance(self):
        """
        Destroy Vast.ai instance
        """
        if self.instance_info:
            logger.info("Shutting down Vast.ai instance...")
            self.vast.destroy_instance(self.instance_info['instance_id'])
            logger.info("Instance destroyed")

def main(manifest_path, structure_path, output_dir="output/stage3_extractions"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 3: KPI EXTRACTION (VAST.AI)")
    logger.info(f"=" * 60)
    
    extractor = VastKPIExtractor(manifest_path, structure_path)
    
    # Extract all pages
    extractions = extractor.extract_all_pages()
    
    logger.info(f"Extraction complete")
    logger.info(f"  Output directory: {extractor.output_dir}")
    
    return extractions

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract KPIs using Vast.ai')
    parser.add_argument('manifest_path', help='Path to image manifest from Stage 2')
    parser.add_argument('structure_path', help='Path to structure JSON from Stage 0')
    parser.add_argument('--output-dir', default='output/stage3_extractions')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.manifest_path, args.structure_path, args.output_dir)