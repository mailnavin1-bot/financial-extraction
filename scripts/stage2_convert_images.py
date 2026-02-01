"""
Stage 2: Convert PDF Pages to Images
Convert flagged pages to PNG images for vision LLM processing
"""

import os
import sys
import logging
from pdf2image import convert_from_path
from PIL import Image

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, save_json, create_output_directories

logger = logging.getLogger(__name__)

class ImageConverter:
    """
    Convert PDF pages to images
    """
    
    def __init__(self, flagged_json_path, pdf_path, output_dir="output/stage2_images"):
        self.flagged_data = load_json(flagged_json_path)
        self.pdf_path = pdf_path
        self.output_dir = output_dir
        self.document_id = self.flagged_data['document_id']
        self.flagged_pages = self.flagged_data['flagged_pages']
        
        # Create document-specific directory
        self.doc_output_dir = os.path.join(output_dir, self.document_id)
        os.makedirs(self.doc_output_dir, exist_ok=True)
    
    def convert_pages(self, dpi=300):
        """
        Convert flagged pages to PNG images
        """
        logger.info(f"Converting {len(self.flagged_pages)} pages to images (DPI: {dpi})...")
        
        images_info = []
        
        # Convert pages in batches to manage memory
        batch_size = 10
        for i in range(0, len(self.flagged_pages), batch_size):
            batch = self.flagged_pages[i:i + batch_size]
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(self.flagged_pages)-1)//batch_size + 1}")
            
            # Convert batch
            # Note: first_page and last_page are 1-indexed
            images = convert_from_path(
                self.pdf_path,
                dpi=dpi,
                first_page=min(batch),
                last_page=max(batch),
                fmt='png'
            )
            
            # Save images
            for page_num in batch:
                # Get corresponding image from batch
                img_idx = page_num - min(batch)
                if img_idx < len(images):
                    img = images[img_idx]
                    
                    # Save
                    filename = f"page_{page_num:03d}.png"
                    filepath = os.path.join(self.doc_output_dir, filename)
                    img.save(filepath, 'PNG', optimize=True)
                    
                    # Get file size
                    size_mb = os.path.getsize(filepath) / (1024 * 1024)
                    
                    images_info.append({
                        'page': page_num,
                        'filename': filename,
                        'filepath': filepath,
                        'size_mb': round(size_mb, 2),
                        'resolution_dpi': dpi,
                        'width': img.width,
                        'height': img.height
                    })
                    
                    logger.debug(f"  ✓ Saved page {page_num}: {filename} ({size_mb:.2f} MB)")
        
        return images_info
    
    def create_manifest(self, images_info):
        """
        Create manifest file
        """
        total_size = sum(img['size_mb'] for img in images_info)
        
        manifest = {
            'document_id': self.document_id,
            'total_images': len(images_info),
            'total_size_mb': round(total_size, 2),
            'output_directory': self.doc_output_dir,
            'images': images_info
        }
        
        return manifest

def main(flagged_json_path, pdf_path, output_dir="output/stage2_images", dpi=300):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 2: IMAGE CONVERSION")
    logger.info(f"=" * 60)
    
    converter = ImageConverter(flagged_json_path, pdf_path, output_dir)
    
    # Convert pages
    images_info = converter.convert_pages(dpi=dpi)
    
    # Create manifest
    manifest = converter.create_manifest(images_info)
    
    # Save manifest
    manifest_file = os.path.join(converter.doc_output_dir, "manifest.json")
    save_json(manifest, manifest_file)
    
    logger.info(f"✓ Images saved to: {converter.doc_output_dir}")
    logger.info(f"  Total images: {manifest['total_images']}")
    logger.info(f"  Total size: {manifest['total_size_mb']} MB")
    logger.info(f"✓ Manifest saved: {manifest_file}")
    
    return manifest

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert PDF pages to images')
    parser.add_argument('flagged_json', help='Path to flagged pages JSON from Stage 1')
    parser.add_argument('pdf_path', help='Path to PDF file')
    parser.add_argument('--output-dir', default='output/stage2_images')
    parser.add_argument('--dpi', type=int, default=300, help='Image resolution (DPI)')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.flagged_json, args.pdf_path, args.output_dir, args.dpi)
