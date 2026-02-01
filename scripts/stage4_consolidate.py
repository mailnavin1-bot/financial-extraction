"""
Stage 4: Consolidate Extractions
Merge all page-level extractions into a single document-level JSON
"""

import os
import sys
import logging
import glob
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, save_json, create_output_directories

logger = logging.getLogger(__name__)

class ExtractionConsolidator:
    """
    Consolidate page-level extractions
    """
    
    def __init__(self, extractions_dir):
        self.extractions_dir = extractions_dir
        self.document_id = os.path.basename(extractions_dir)
    
    def consolidate(self):
        """
        Consolidate all page extractions
        """
        # Find all extraction files
        extraction_files = sorted(glob.glob(
            os.path.join(self.extractions_dir, "page_*_extraction.json")
        ))
        
        logger.info(f"Consolidating {len(extraction_files)} extraction files...")
        
        all_extractions = []
        conflicts = []
        
        # Load all extractions
        for filepath in extraction_files:
            try:
                data = load_json(filepath)
                
                if 'extractions' in data:
                    all_extractions.extend(data['extractions'])
                
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
                continue
        
        logger.info(f"Total raw extractions: {len(all_extractions)}")
        
        # Deduplicate by (kpi_name, fiscal_year)
        deduplicated, conflicts = self._deduplicate_extractions(all_extractions)
        
        logger.info(f"After deduplication: {len(deduplicated)} unique KPIs")
        if conflicts:
            logger.warning(f"Found {len(conflicts)} conflicts")
        
        # Calculate statistics
        stats = self._calculate_statistics(deduplicated)
        
        # Build output
        consolidated = {
            'document_id': self.document_id,
            'total_pages_processed': len(extraction_files),
            'total_raw_extractions': len(all_extractions),
            'total_unique_extractions': len(deduplicated),
            'extractions': deduplicated,
            'conflicts': conflicts,
            'statistics': stats
        }
        
        return consolidated
    
    def _deduplicate_extractions(self, extractions):
        """
        Deduplicate extractions by (kpi_name, fiscal_year)
        Keep highest confidence version
        """
        # Group by (kpi_name, fiscal_year)
        groups = defaultdict(list)
        
        for extraction in extractions:
            key = (
                extraction.get('kpi_name', 'unknown'),
                extraction.get('fiscal_year', 0)
            )
            groups[key].append(extraction)
        
        deduplicated = []
        conflicts = []
        
        for key, group in groups.items():
            if len(group) == 1:
                # No conflict
                deduplicated.append(group[0])
            else:
                # Multiple extractions for same KPI + year
                # Check if values are the same
                values = [e.get('value_numeric') for e in group]
                unique_values = set(values)
                
                if len(unique_values) == 1:
                    # Same value, different sources - keep highest confidence
                    best = max(group, key=lambda x: x.get('confidence', 0))
                    
                    # Add cross-validation note
                    sources = [e.get('source', {}).get('page', 'unknown') for e in group]
                    best['confidence'] = min(best['confidence'] * 1.1, 1.0)  # Boost confidence
                    if 'extraction_notes' not in best:
                        best['extraction_notes'] = ''
                    best['extraction_notes'] += f" | Confirmed across pages: {sources}"
                    
                    deduplicated.append(best)
                else:
                    # Different values - conflict!
                    best = max(group, key=lambda x: x.get('confidence', 0))
                    
                    conflict = {
                        'kpi_name': key[0],
                        'fiscal_year': key[1],
                        'values': [
                            {
                                'value': e.get('value_numeric'),
                                'source_page': e.get('source', {}).get('page'),
                                'confidence': e.get('confidence')
                            } for e in group
                        ],
                        'resolution': f"Selected value {best.get('value_numeric')} (highest confidence)"
                    }
                    conflicts.append(conflict)
                    
                    # Mark as needing review
                    best['flags'] = best.get('flags', {})
                    best['flags']['conflicting_values'] = True
                    best['flags']['needs_review'] = True
                    
                    deduplicated.append(best)
        
        return deduplicated, conflicts
    
    def _calculate_statistics(self, extractions):
        """
        Calculate summary statistics
        """
        if not extractions:
            return {}
        
        # By category
        by_category = defaultdict(int)
        for e in extractions:
            category = e.get('kpi_category', 'unknown')
            by_category[category] += 1
        
        # By fiscal year
        by_year = defaultdict(int)
        for e in extractions:
            year = e.get('fiscal_year', 0)
            by_year[year] += 1
        
        # Confidence distribution
        confidences = [e.get('confidence', 0) for e in extractions]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        high_confidence = sum(1 for c in confidences if c >= 0.85)
        medium_confidence = sum(1 for c in confidences if 0.70 <= c < 0.85)
        low_confidence = sum(1 for c in confidences if c < 0.70)
        
        needs_review = sum(1 for e in extractions if e.get('flags', {}).get('needs_review', False))
        
        return {
            'kpis_by_category': dict(by_category),
            'kpis_by_fiscal_year': dict(by_year),
            'fiscal_years_covered': sorted(by_year.keys(), reverse=True),
            'average_confidence': round(avg_confidence, 3),
            'high_confidence_count': high_confidence,
            'medium_confidence_count': medium_confidence,
            'low_confidence_count': low_confidence,
            'needs_review_count': needs_review
        }

def main(extractions_dir, output_dir="output/stage4_consolidated"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 4: CONSOLIDATION")
    logger.info(f"=" * 60)
    
    consolidator = ExtractionConsolidator(extractions_dir)
    consolidated = consolidator.consolidate()
    
    # Save
    output_file = os.path.join(output_dir, f"{consolidator.document_id}_consolidated.json")
    save_json(consolidated, output_file)
    
    logger.info(f"✓ Consolidated extraction saved: {output_file}")
    logger.info(f"  Unique KPIs: {consolidated['total_unique_extractions']}")
    logger.info(f"  Conflicts: {len(consolidated['conflicts'])}")
    logger.info(f"  Avg confidence: {consolidated['statistics']['average_confidence']}")
    logger.info(f"  Needs review: {consolidated['statistics']['needs_review_count']}")
    
    return consolidated

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Consolidate page extractions')
    parser.add_argument('extractions_dir', help='Directory containing page extraction JSONs')
    parser.add_argument('--output-dir', default='output/stage4_consolidated')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.extractions_dir, args.output_dir)
