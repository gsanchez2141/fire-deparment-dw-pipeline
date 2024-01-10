import logging
from datetime import datetime
from io import BytesIO
from typing import List, Tuple, Optional
import polars as pl

import boto3


class Utils:
    @staticmethod
    def list_files_in_s3(s3_client: boto3.client, bucket_name: str) -> List[Tuple[str, datetime]]:
        # List files in the S3 bucket
        response = s3_client.list_objects(Bucket=bucket_name)

        # Extract file names and last modified timestamps
        files = response.get('Contents', [])
        files_info = [(file['Key'], file['LastModified']) for file in files]

        return files_info

    @staticmethod
    def get_latest_file_name(files_info: List[Tuple[str, datetime]]) -> Optional[str]:
        # Check if there are any files
        if not files_info:
            return None

        # Get the file with the maximum last modified timestamp
        latest_file = max(files_info, key=lambda x: x[1])
        return latest_file[0]

    @staticmethod
    def get_file_from_s3_as_dataframe(s3_client: boto3.client, bucket_name: str, filename: str,
                                      schema: dict) -> pl.DataFrame:
        # Read existing CSV file from S3 if available
        if filename:
            response = s3_client.get_object(Bucket=bucket_name, Key=filename)
            data_dataframe = pl.read_csv(response['Body'], dtypes=schema)
            return data_dataframe
        else:
            # If no existing file, create an empty DataFrame
            data_dataframe = pl.DataFrame(schema=schema)
            return data_dataframe

    @staticmethod
    def check_and_log_error(dataframe: pl.DataFrame):
        error_message = "Repeated values found in one of the main fields."

        if dataframe.shape[0] > 0:
            logging.INFO(dataframe)
            logging.error(f"Error: {error_message}")
            raise ValueError(error_message)

    @staticmethod
    def send_csv_to_s3(
            s3_client: boto3.client, bucket_name: str, new_latest_file_name: str, combined_dataframe: pl.DataFrame
    ) -> None:
        # Write the combined DataFrame back to S3
        buffer = BytesIO()
        combined_dataframe.write_csv(buffer)

        # Seek to the beginning of the buffer before sending to S3
        buffer.seek(0)

        s3_client.put_object(Bucket=bucket_name, Key=new_latest_file_name, Body=buffer.read())
