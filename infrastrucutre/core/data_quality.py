"""
Data Quality Module for Donor Analytics Pipeline
Implements validation rules and monitoring for data quality
"""
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import yaml
import great_expectations as ge

class DataQualityChecker:
    def __init__(self, config_path: str = 'configs/data_quality.yml'):
        """Initialize with configuration"""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.validation_results = []
    
    def _load_config(self, config_path: str) -> Dict:
        """Load data quality rules from config"""
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def validate_table(self, df: pd.DataFrame, table_name: str) -> bool:
        """Run all configured validations for a table"""
        ge_df = ge.from_pandas(df)
        rules = self.config['tables'].get(table_name, {})
        
        results = []
        for rule in rules:
            rule_type = rule['type']
            
            if rule_type == 'not_null':
                result = ge_df.expect_column_values_to_not_be_null(
                    rule['column'],
                    mostly=rule.get('threshold', 1.0)
                )
            
            elif rule_type == 'unique':
                result = ge_df.expect_column_values_to_be_unique(
                    rule['column']
                )
            
            elif rule_type == 'range':
                result = ge_df.expect_column_values_to_be_between(
                    rule['column'],
                    min_value=rule.get('min'),
                    max_value=rule.get('max')
                )
            
            elif rule_type == 'format':
                result = ge_df.expect_column_values_to_match_regex(
                    rule['column'],
                    rule['pattern']
                )
            
            results.append({
                'table': table_name,
                'rule': rule,
                'success': result.success,
                'result': result.result
            })
        
        self.validation_results.extend(results)
        return all(r['success'] for r in results)
    
    def validate_referential_integrity(
        self, 
        parent_df: pd.DataFrame,
        child_df: pd.DataFrame,
        parent_key: str,
        child_key: str
    ) -> bool:
        """Check referential integrity between tables"""
        parent_keys = set(parent_df[parent_key].unique())
        child_keys = set(child_df[child_key].unique())
        
        orphaned_keys = child_keys - parent_keys
        if orphaned_keys:
            self.logger.error(
                f"Referential integrity violation: {len(orphaned_keys)} orphaned keys found"
            )
            return False
        return True
    
    def check_duplicate_donors(self, donors_df: pd.DataFrame) -> pd.DataFrame:
        """Identify potential duplicate donor records"""
        # Fuzzy match on name + location
        from fuzzywuzzy import fuzz
        
        def name_distance(row1, row2):
            name_score = fuzz.ratio(
                f"{row1['first_name']} {row1['last_name']}",
                f"{row2['first_name']} {row2['last_name']}"
            )
            location_score = fuzz.ratio(
                f"{row1['city']} {row1['state']}",
                f"{row2['city']} {row2['state']}"
            )
            return (name_score * 0.7 + location_score * 0.3) / 100
        
        duplicates = []
        for i, row1 in donors_df.iterrows():
            for j, row2 in donors_df.iterrows():
                if i < j:  # Avoid duplicate comparisons
                    similarity = name_distance(row1, row2)
                    if similarity > 0.9:  # High similarity threshold
                        duplicates.append({
                            'donor_id_1': row1['donor_id'],
                            'donor_id_2': row2['donor_id'],
                            'similarity': similarity
                        })
        
        return pd.DataFrame(duplicates)
    
    def generate_quality_report(self, output_path: str = None) -> Dict:
        """Generate a summary report of data quality checks"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': all(r['success'] for r in self.validation_results),
            'total_checks': len(self.validation_results),
            'passed_checks': sum(1 for r in self.validation_results if r['success']),
            'failed_checks': sum(1 for r in self.validation_results if not r['success']),
            'details': self.validation_results
        }
        
        if output_path:
            with open(output_path, 'w') as f:
                yaml.dump(report, f)
        
        return report

    def monitor_data_drift(
        self,
        current_df: pd.DataFrame,
        historical_df: pd.DataFrame,
        columns: List[str]
    ) -> Dict:
        """Monitor statistical drift in key metrics"""
        from scipy import stats
        
        drift_results = {}
        for col in columns:
            if pd.api.types.is_numeric_dtype(current_df[col]):
                # KS test for numerical columns
                stat, pvalue = stats.ks_2samp(
                    current_df[col].dropna(),
                    historical_df[col].dropna()
                )
                drift_results[col] = {
                    'test': 'ks_test',
                    'statistic': stat,
                    'p_value': pvalue,
                    'significant_drift': pvalue < 0.05
                }
            else:
                # Chi-square test for categorical columns
                current_counts = current_df[col].value_counts()
                historical_counts = historical_df[col].value_counts()
                
                # Align categories
                all_categories = list(set(current_counts.index) | set(historical_counts.index))
                current_counts = current_counts.reindex(all_categories).fillna(0)
                historical_counts = historical_counts.reindex(all_categories).fillna(0)
                
                stat, pvalue = stats.chisquare(current_counts, historical_counts)
                drift_results[col] = {
                    'test': 'chi_square',
                    'statistic': stat,
                    'p_value': pvalue,
                    'significant_drift': pvalue < 0.05
                }
        
        return drift_results