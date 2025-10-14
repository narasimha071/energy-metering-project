#Combining all raw data into unique dataset groupby Date
import boto3
import json
import csv
import os

# AWS Config
BUCKET_NAME = "meteringbucket1234"
REGION = "ap-south-1"

s3 = boto3.client(
    's3',
    aws_access_key_id='Your Access Key',
    aws_secret_access_key='Your Secret Key',
    region_name=REGION
)

output_dir = "metering_csv"
os.makedirs(output_dir, exist_ok=True)

# Single CSV file for all data
csv_file = os.path.join(output_dir, "combined_metering.csv")

# List all objects recursively
paginator = s3.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=BUCKET_NAME)
for page in page_iterator:
    for obj in page.get('Contents', []):
        file_key = obj['Key']
        # Skip if it's a folder
        if file_key.endswith("/"):
            continue
        # Read S3 object
        obj_body = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        data = obj_body['Body'].read().decode('utf-8').strip().split("\n")

        # Write to CSV (append mode)
        with open(csv_file, "a", newline="") as f:
            writer = None
            for line in data:
                record = json.loads(line)
                row = list(record.values())[0].split(",")
                if writer is None:
                    header = list(record.keys())[0].split(",")
                    writer = csv.writer(f)
                    if os.stat(csv_file).st_size == 0:
                        writer.writerow(header)
                writer.writerow(row)
        print(f"Processed {file_key} -> {csv_file}")
