import os
from dotenv import load_dotenv
import boto3

load_dotenv()

# Database configuration
DB_CONFIG = {
    'server': os.getenv("DB_SERVER"),
    'database': os.getenv("DB_NAME"),
    'username': os.getenv("DB_USERNAME"),
    'password': os.getenv("DB_PASSWORD"),
    'port': int(os.getenv("DB_PORT", 3306)),
}

# AWS S3 configuration
AWS_CONFIG = {
    'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),
    'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
    'region_name': os.getenv("AWS_REGION_NAME", 'us-west-1'),
    'bucket_name': os.getenv("AWS_BUCKET_NAME")
}

# Optionally initialize S3 client if needed elsewhere
S3_CLIENT = boto3.client(
    's3',
    aws_access_key_id=AWS_CONFIG['aws_access_key_id'],
    aws_secret_access_key=AWS_CONFIG['aws_secret_access_key'],
    region_name=AWS_CONFIG['region_name']
)
