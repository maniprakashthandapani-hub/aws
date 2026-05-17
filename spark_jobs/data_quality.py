import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, array, struct, to_json
from pyspark.sql.types import BooleanType

class DataQualityEngine:
    """
    Applies row-level Data Quality rules defined in a JSON file.
    Splits the data into 'valid' and 'rejected' DataFrames.
    """
    
    def __init__(self, rules_path: str):
        with open(rules_path, 'r') as f:
            self.config = json.load(f)
            self.rules = self.config.get('rules', [])

    def apply_rules(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Evaluates rules and appends a 'dq_failed_rules' array column.
        Returns (valid_df, rejected_df)
        """
        # Create an expression that evaluates each rule
        # We will build an array of failed rule IDs.
        
        # Start with an empty array of string
        df_with_dq = df.withColumn("dq_failed_rules", array().cast("array<string>"))
        
        for rule in self.rules:
            rule_id = rule['rule_id']
            column_name = rule['column']
            rule_type = rule['type']
            
            # Skip rules for columns that don't exist
            if column_name not in df.columns:
                continue
                
            condition = None
            
            if rule_type == 'not_null':
                condition = col(column_name).isNull()
                
            elif rule_type == 'range':
                min_val = rule.get('min')
                max_val = rule.get('max')
                if min_val is not None and max_val is not None:
                    condition = (col(column_name) < lit(min_val)) | (col(column_name) > lit(max_val))
                elif min_val is not None:
                    condition = col(column_name) < lit(min_val)
                elif max_val is not None:
                    condition = col(column_name) > lit(max_val)
                    
            elif rule_type == 'regex':
                pattern = rule.get('pattern')
                if pattern:
                    # RLIKE returns true if it matches. Condition is true if it FAILS.
                    # Also ignore nulls (let not_null handle null checks if required)
                    condition = col(column_name).isNotNull() & ~col(column_name).rlike(pattern)
            
            if condition is not None:
                # Append rule_id to the array if condition is met (rule failed)
                df_with_dq = df_with_dq.withColumn(
                    "dq_failed_rules",
                    when(condition, 
                         pyspark_array_append(col("dq_failed_rules"), lit(rule_id))
                    ).otherwise(col("dq_failed_rules"))
                )

        # Split the data
        from pyspark.sql.functions import size
        
        # Valid: array size is 0
        valid_df = df_with_dq.filter(size(col("dq_failed_rules")) == 0).drop("dq_failed_rules")
        
        # Rejected: array size > 0
        rejected_df = df_with_dq.filter(size(col("dq_failed_rules")) > 0)
        
        return valid_df, rejected_df

def pyspark_array_append(arr_col, val_col):
    """Helper to append an element to an array column"""
    from pyspark.sql.functions import concat, array
    return concat(arr_col, array(val_col))
