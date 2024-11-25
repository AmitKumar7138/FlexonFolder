import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError
from data_processing import DataProcessing
from upload_code_to_s3 import upload_file_to_s3, zip_files
from config import AWS_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename="ELT_log.txt",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def download_file_from_s3(bucket_name, s3_key, local_path):
    """
    Downloads a file from S3 to the local file system.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_CONFIG['aws_access_key_id'],
        aws_secret_access_key=AWS_CONFIG['aws_secret_access_key'],
        region_name=AWS_CONFIG['region_name']
    )
    try:
        s3_client.download_file(bucket_name, s3_key, local_path)
        print(f"Downloaded {s3_key} from s3://{bucket_name} to {local_path}")
        logging.info(f"Downloaded {s3_key} from s3://{bucket_name} to {local_path}")
    except FileNotFoundError:
        print(f"The file {local_path} could not be saved locally.")
        logging.error(f"The file {local_path} could not be saved locally.")
    except NoCredentialsError:
        print("AWS credentials not available.")
        logging.error("AWS credentials not available.")
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        logging.error(f"Error downloading file: {str(e)}")


def main():

    #Define S3 bucket and file details
    bucket_name = AWS_CONFIG['bucket_name']
    s3_key = "original_data/TitanicData.csv"
    local_csv_path = "Titanic-Dataset.csv"

    # Download the CSV file from S3
    download_file_from_s3(bucket_name, s3_key, local_csv_path)

    # Process the CSV file
    data_processor = DataProcessing()
    table_name = "TitanicData"

    data_processor.create_table_from_csv(local_csv_path, table_name)
    data_processor.data_preprocessing(table_name)
    data_processor.close_connection()

    # Upload code files as zip
    code_files = [
        'main.py',
        'database_connection.py',
        'data_processing.py',
        'config.py',
        'upload_code_to_s3.py'
        # Exclude 'config.py' if necessary
    ]
    zip_name = 'code_backup.zip'
    zip_files(code_files, zip_name)

    s3_key_backup = f"code_backup/{zip_name}"
    upload_file_to_s3(bucket_name, s3_key_backup, zip_name)

    # Remove the zip file after uploading
    os.remove(zip_name)

if __name__ == "__main__":
    main()
