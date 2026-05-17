import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, lit

class SchemaValidator:
    """
    Validates DataFrame schemas against a JSON contract and applies basic standardisation
    (null filling, missing column flagging).
    """
    
    # Map JSON types to PySpark types
    TYPE_MAPPING = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType()
    }

    def __init__(self, contract_path: str):
        with open(contract_path, 'r') as f:
            self.contract = json.load(f)
            
    def get_pyspark_schema(self) -> StructType:
        """Converts the JSON contract into a PySpark StructType."""
        fields = []
        for col_def in self.contract['columns']:
            spark_type = self.TYPE_MAPPING.get(col_def['type'], StringType())
            # We allow nullable at the schema read level so we can catch and flag nulls later
            fields.append(StructField(col_def['name'], spark_type, True))
        return StructType(fields)

    def validate_and_standardise(self, df: DataFrame) -> DataFrame:
        """
        Applies the contract rules to the DataFrame.
        - Fills defaults for 'fill' strategy
        - Creates boolean flags for 'flag' strategy
        - Validates 'required' columns
        """
        expected_cols = {c['name'] for c in self.contract['columns']}
        actual_cols = set(df.columns)
        
        # 1. Structural Validation
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            raise ValueError(f"Schema Validation Failed. Missing required columns: {missing_cols}")
            
        # 2. Apply Null Strategies
        for col_def in self.contract['columns']:
            c_name = col_def['name']
            strategy = col_def.get('nullStrategy', 'drop')
            
            # If required=true, any nulls here should fail the row later in DQ checks
            # We don't drop rows here; we let the DQ framework handle bad rows.
            
            if strategy == 'fill' and 'fillValue' in col_def:
                df = df.fillna({c_name: col_def['fillValue']})
                
            elif strategy == 'flag':
                # Create a boolean column indicating if the original value was null
                df = df.withColumn(f"is_{c_name}_null", col(c_name).isNull())
                
            elif strategy == 'fill_and_flag' and 'fillValue' in col_def:
                df = df.withColumn(f"is_{c_name}_defaulted", col(c_name).isNull())
                df = df.fillna({c_name: col_def['fillValue']})
                
        return df
