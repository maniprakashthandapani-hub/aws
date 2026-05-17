import csv
import random
from datetime import datetime, timedelta

def generate_data(num_mb=100):
    output_file = 'data/sample/large_input.csv'
    target_bytes = num_mb * 1024 * 1024
    
    # UTF-8 is the industry standard for data pipelines.
    # Spark can split UTF-8 files across multiple executors for parallel processing.
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        # EXACT COLUMN ORDER FROM SCHEMA DEFINITION
        writer.writerow(['customer_id', 'name', 'email', 'phone', 'transaction_date', 'amount'])
        
        bytes_written = 0
        batch_size = 10000
        start_date = datetime(2025, 1, 1)
        
        while bytes_written < target_bytes:
            batch = []
            for _ in range(batch_size):
                # Target schema: customer_id (integer)
                c_id = random.randint(10000, 99999)
                name = f"User_{random.randint(1000, 9999)}"
                email = f"user_{random.randint(1000, 9999)}@example.com"
                phone = f"+447700900{random.randint(100, 999)}"
                t_date = (start_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
                amount = round(random.uniform(10.0, 5000.0), 2)
                
                # Introduce a few bad rows (1% failure rate)
                if random.random() < 0.01:
                    amount = -10.0  # Fails RQ_001
                if random.random() < 0.01:
                    email = "bad_email" # Fails RQ_004
                
                row = [c_id, name, email, phone, t_date, amount]
                batch.append(row)
                
            writer.writerows(batch)
            bytes_written += batch_size * 180 # roughly 180 bytes per row in utf-16
            
            if bytes_written % (10 * 1024 * 1024) < (batch_size * 180):
                print(f"Generated {bytes_written / (1024*1024):.1f} MB...")
                
    print(f"Done! File saved to {output_file}")

if __name__ == '__main__':
    generate_data(100)
