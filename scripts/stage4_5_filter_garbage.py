"""
Stage 4.5: Garbage Filter
Remove invalid/garbage KPIs before normalization
Implements Gemini's critique about filtering noise
"""

import os
import sys
import logging
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, save_json, create_output_directories

logger = logging.getLogger(__name__)

class GarbageFilter:
    """
    Filter out garbage/invalid KPI extractions
    """
    
    # Blacklist patterns for KPI names
    BLACKLIST_PREFIXES = [
        'table', 'schedule', 'note', 'annexure',
        'figure', 'chart', 'graph', 'page', 'section',
        'appendix', 'exhibit'
    ]
    
    BLACKLIST_CONTAINS = [
        'as per', 'in accordance', 'refer to',
        '...', '---', '***', '___',
        'total assets', 'total liabilities', 'revenue',
        'profit', 'loss', 'balance sheet'
    ]
    
    # Valid units whitelist
    VALID_UNITS = [
        # Counts
        'employees', 'count', 'number', 'units', 'branches',
        'stores', 'customers', 'subscribers', 'users', 'accounts',
        
        # Currency
        'inr', 'usd', 'eur', 'gbp', 'crores', 'millions', 'lakhs',
        'billions', 'thousands',
        
        # Percentages
        'percentage', '%', 'percent',
        
        # Rates
        'per month', 'per annum', 'per day', 'arpu',
        
        # Volumes
        'tons', 'kg', 'kilograms', 'liters', 'mw', 'kw',
        'sq.ft.', 'acres', 'hectares', 'units'
    ]
    
    def __init__(self, consolidated_path):
        self.data = load_json(consolidated_path)
        self.document_id = self.data['document_id']
        self.extractions = self.data['extractions']
    
    def filter_extractions(self):
        """
        Apply all filters to remove garbage
        """
        logger.info(f"Filtering {len(self.extractions)} extractions...")
        
        filtered = []
        discarded = []
        
        for extraction in self.extractions:
            is_valid, reason = self._validate_extraction(extraction)
            
            if is_valid:
                filtered.append(extraction)
            else:
                discarded.append({
                    'extraction': extraction,
                    'reason': reason
                })
        
        logger.info(f"Results:")
        logger.info(f"  ✓ Valid: {len(filtered)} ({len(filtered)/len(self.extractions)*100:.1f}%)")
        logger.info(f"  ✗ Discarded: {len(discarded)} ({len(discarded)/len(self.extractions)*100:.1f}%)")
        
        # Log discard reasons
        discard_reasons = {}
        for d in discarded:
            reason = d['reason']
            discard_reasons[reason] = discard_reasons.get(reason, 0) + 1
        
        if discard_reasons:
            logger.info(f"  Discard breakdown:")
            for reason, count in sorted(discard_reasons.items(), key=lambda x: -x[1]):
                logger.info(f"    {reason}: {count}")
        
        return filtered, discarded
    
    def _validate_extraction(self, extraction):
        """
        Validate a single extraction
        Returns: (is_valid, reason)
        """
        kpi_name = extraction.get('kpi_name', '')
        value_numeric = extraction.get('value_numeric')
        unit = extraction.get('unit', '')
        confidence = extraction.get('confidence', 0)
        category = extraction.get('kpi_category', '')
        
        # Filter 1: KPI name length
        if len(kpi_name) > 50:
            return False, 'name_too_long'
        if len(kpi_name) < 3:
            return False, 'name_too_short'
        
        # Filter 2: Blacklist patterns
        kpi_lower = kpi_name.lower()
        
        for prefix in self.BLACKLIST_PREFIXES:
            if kpi_lower.startswith(prefix):
                return False, f'blacklist_prefix_{prefix}'
        
        for phrase in self.BLACKLIST_CONTAINS:
            if phrase in kpi_lower:
                return False, f'blacklist_contains'
        
        # Check if just a number
        if re.match(r'^[0-9.,]+$', kpi_name):
            return False, 'just_a_number'
        
        # Filter 3: Value validity
        if value_numeric is None:
            # Exception: qualitative disclosures are OK
            if category == 'forward_looking' or category == 'governance':
                pass  # Allow null values for these categories
            else:
                return False, 'no_numeric_value'
        
        # Filter 4: Unit validity
        if not self._is_valid_unit(unit):
            return False, 'invalid_unit'
        
        # Filter 5: Confidence threshold
        if confidence < 0.50:
            return False, 'low_confidence'
        
        # All checks passed
        return True, None
    
    def _is_valid_unit(self, unit):
        """
        Check if unit is valid
        """
        if not unit or unit.strip() == '':
            return False
        
        unit_lower = unit.lower().strip()
        
        # Exact match
        if unit_lower in self.VALID_UNITS:
            return True
        
        # Partial match (e.g., "INR Crores" contains "crores")
        for valid_unit in self.VALID_UNITS:
            if valid_unit in unit_lower:
                return True
        
        return False
    
    def build_output(self, filtered, discarded):
        """
        Build filtered output JSON
        """
        # Recalculate statistics
        from stage4_consolidate import ExtractionConsolidator
        consolidator = ExtractionConsolidator(None)
        stats = consolidator._calculate_statistics(filtered)
        
        output = {
            'document_id': self.document_id,
            'total_before_filtering': len(self.extractions),
            'total_after_filtering': len(filtered),
            'total_discarded': len(discarded),
            'discard_rate': round(len(discarded) / len(self.extractions), 3) if self.extractions else 0,
            'extractions': filtered,
            'discarded': [
                {
                    'kpi_name': d['extraction'].get('kpi_name'),
                    'reason': d['reason']
                } for d in discarded
            ],
            'statistics': stats
        }
        
        return output

def main(consolidated_path, output_dir="output/stage4_5_filtered"):
    """
    Main execution
    """
    logger.info(f"=" * 60)
    logger.info(f"STAGE 4.5: GARBAGE FILTER")
    logger.info(f"=" * 60)
    
    filter_obj = GarbageFilter(consolidated_path)
    filtered, discarded = filter_obj.filter_extractions()
    
    # Build output
    output = filter_obj.build_output(filtered, discarded)
    
    # Save
    output_file = os.path.join(output_dir, f"{filter_obj.document_id}_filtered.json")
    save_json(output, output_file)
    
    logger.info(f"✓ Filtered data saved: {output_file}")
    logger.info(f"  Valid KPIs: {output['total_after_filtering']}")
    logger.info(f"  Discard rate: {output['discard_rate']*100:.1f}%")
    
    return output

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Filter garbage KPIs')
    parser.add_argument('consolidated_path', help='Path to consolidated JSON from Stage 4')
    parser.add_argument('--output-dir', default='output/stage4_5_filtered')
    
    args = parser.parse_args()
    
    setup_logging()
    create_output_directories()
    
    main(args.consolidated_path, args.output_dir)
