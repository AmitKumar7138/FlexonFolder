import os
import boto3
from botocore.exceptions import NoCredentialsError
from config import AWS_CONFIG
import logging
import zipfile

def zip_files(file_paths, zip_name):
    """
    Create a ZIP file containing the specified files and folders.
    """
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for path in file_paths:
            if os.path.isdir(path):
                # If the path is a directory, recursively add its contents
                for foldername, subfolders, filenames in os.walk(path):
                    for filename in filenames:
                        file_path = os.path.join(foldername, filename)
                        arcname = os.path.relpath(file_path, start=os.path.dirname(path))
                        zipf.write(file_path, arcname)
            else:
                # If it's a file, add it directly
                zipf.write(path)
    return zip_name

def upload_file_to_s3(bucket_name, s3_key, file_path):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_CONFIG['aws_access_key_id'],
        aws_secret_access_key=AWS_CONFIG['aws_secret_access_key'],
        region_name=AWS_CONFIG['region_name']
    )
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        logging.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        logging.error(f"The file {file_path} was not found.")
    except NoCredentialsError:
        print("AWS credentials not available.")
        logging.error("AWS credentials not available.")

if __name__ == "__main__":
    # List of code files to upload
    code_files = [
        'main.py',
        'database_connection.py',
        'data_processing.py',
        'config.py',
        'upload_code_to_s3.py'
        # Exclude 'config.py' if it contains sensitive information
    ]
    zip_name = 'code_backup.zip'
    zip_files(code_files, zip_name)

    # S3 bucket details
    bucket_name = AWS_CONFIG['bucket_name']
    s3_key = f"code_backup/{zip_name}"

    # Upload zip file
    upload_file_to_s3(bucket_name, s3_key, zip_name)

    # Remove the zip file after uploading
    os.remove(zip_name)
