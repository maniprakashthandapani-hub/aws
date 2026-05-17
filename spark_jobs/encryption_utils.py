import boto3
import base64
from pyspark.sql.functions import col, expr

class KMSEnvelopeEncryption:
    """
    Implements AWS KMS Envelope Encryption for PySpark.
    
    1. Calls AWS KMS to generate an AES-256 Data Key.
    2. Uses the Plaintext Key to encrypt specific columns in the DataFrame using Spark's native aes_encrypt.
    3. Provides the Ciphertext Key to be saved safely to S3.
    """

    def __init__(self, kms_key_arn: str, region_name: str = 'eu-west-2'):
        self.kms_key_arn = kms_key_arn
        # Initialize boto3 client. On EMR Serverless, this automatically uses the Execution Role.
        self.kms_client = boto3.client('kms', region_name=region_name)
        self.plaintext_key = None
        self.ciphertext_key = None

    def generate_data_key(self):
        """Generates a new AES-256 data key from KMS."""
        response = self.kms_client.generate_data_key(
            KeyId=self.kms_key_arn,
            KeySpec='AES_256'
        )
        # Plaintext is bytes. We convert to hex for PySpark's unhex() function.
        self.plaintext_key = response['Plaintext'].hex()
        # CiphertextBlob is bytes. We encode to base64 for easy storage in JSON/text.
        self.ciphertext_key = base64.b64encode(response['CiphertextBlob']).decode('utf-8')
        return self.ciphertext_key

    def encrypt_spii_columns(self, df, columns_to_encrypt):
        """
        Encrypts the specified columns using Spark 3's native aes_encrypt function.
        Replaces the plaintext column with the base64-encoded encrypted string.
        """
        if not self.plaintext_key:
            raise ValueError("Data key not generated. Call generate_data_key() first.")

        if not columns_to_encrypt:
            return df

        for column in columns_to_encrypt:
            # Spark's aes_encrypt returns binary. We base64 encode it so it can be stored as a string in Parquet.
            # Using GCM mode which is the default in newer Spark versions and highly secure.
            encryption_expr = f"base64(aes_encrypt({column}, unhex('{self.plaintext_key}')))"
            
            # Replace the column with its encrypted version
            # We use expr() to run the SQL function directly
            df = df.withColumn(column, expr(encryption_expr))
            
        return df

    def get_ciphertext_blob(self):
        """Returns the encrypted data key for secure storage."""
        return self.ciphertext_key
