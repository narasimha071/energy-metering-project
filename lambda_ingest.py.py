import pandas as pd
import boto3
import json
import hashlib
from datetime import datetime

# File path
file_path = r'C:\Users\rajuc\OneDrive\Desktop\Project-02\output.csv'

# AWS Kinesis details
kinesis_stream_name = 'Plant-Metering'
aws_region = 'ap-south-1'
partition_key_field = 'Date'

print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading CSV file...")

# Read file with correct delimiter
df = pd.read_csv(file_path, delimiter=';')
print(f"✓ Loaded {len(df)} records")
print(f"Columns: {list(df.columns)}")

# Set up Kinesis client
kinesis = boto3.client('kinesis', region_name=aws_region)
print(f"✓ Connected to Kinesis")

# Counters
total = len(df)
successful = 0
failed = 0

print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Sending records one by one...")

# Send each row to Kinesis
for idx, row in df.iterrows():
    try:
        record = row.to_dict()
        
        # Fix 1: Handle NaN values in the record
        clean_record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        
        # Fix 2: Create a valid partition key
        partition_key_value = record.get(partition_key_field)
        
        # Handle various partition key issues
        if pd.isna(partition_key_value) or partition_key_value is None:
            # Use row index if date is missing
            partition_key = f"record-{idx}"
        else:
            # Convert to string and remove any problematic characters
            partition_key = str(partition_key_value).strip()
            # Remove special characters that might cause issues
            partition_key = partition_key.replace('/', '-').replace(':', '-').replace(' ', '_')
            
            # If partition key is empty after cleaning, use hash of record
            if not partition_key:
                partition_key = hashlib.md5(json.dumps(clean_record, sort_keys=True).encode()).hexdigest()[:16]
        
        # Send to Kinesis
        kinesis.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(clean_record, default=str),
            PartitionKey=partition_key
        )
        
        successful += 1
        
        # Progress update every 1000 records
        if (idx + 1) % 1000 == 0:
            print(f"  Progress: {idx + 1}/{total} ({(idx + 1)/total*100:.1f}%)")
            
    except Exception as e:
        failed += 1
        print(f"✗ Error at record {idx}: {e}")
        if failed < 10:  # Only show first 10 errors
            print(f"   Record data: {record}")

# Final summary
print(f"\n{'='*60}")
print(f"[{datetime.now().strftime('%H:%M:%S')}] Upload Complete!")
print(f"{'='*60}")
print(f"Total Records:     {total}")
print(f"✓ Successful:      {successful}")
print(f"✗ Failed:          {failed}")
print(f"{'='*60}")

if failed > 0:
    print(f"\n⚠ Warning: {failed} records failed")
    print(f"Check the error messages above for details")