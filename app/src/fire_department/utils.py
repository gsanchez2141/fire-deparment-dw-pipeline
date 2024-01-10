from datetime import datetime
from io import StringIO
from typing import Tuple, List, Optional
import boto3
import pandas as pd
import logging
import pg8000


class Utils:
    @staticmethod
    def connect_to_postgres(db_host, db_port, db_user, db_password, db_name):

        print()
        try:
            conn = pg8000.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_name
            )
            return conn
        except Exception as e:
            logging.error(f"Error creating PostgreSQL connection: {e}")
            raise  # Re-raise the exception

    @staticmethod
    def s3_bucket_object_read(bucket, file_name):
        response = bucket.Object(file_name)
        # method reads from s3 in bytes, needs to be decoded for it to be turned into string
        data = response.get()["Body"].read()  # .decode()
        if not data:
            return None
        return data

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
                                      FireDepartmentModel) -> pd.DataFrame:
        try:
            # Read existing CSV file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=filename)
            csv_data = pd.read_csv(response['Body'])

            # Define a function to normalize column names
            def normalize_column_name(column_name):
                return column_name.lower().replace(' ', '_')

            # Apply the function to normalize all column names
            csv_data.columns = csv_data.columns.map(normalize_column_name)

            # Convert CSV data to FireDepartmentModel instances
            data_instances = []
            for _, row in csv_data.iterrows():
                data_dict = row.to_dict()
                data_instance = FireDepartmentModel(**data_dict)
                data_instances.append(data_instance)

            # Convert data instances to Pandas DataFrame
            df = pd.DataFrame([data.__dict__ for data in data_instances])

            return df

        except Exception as e:

            logging.error(f"Error: {e}")

        return pd.DataFrame()

    @staticmethod
    def clean_str(x):
        return x.upper().strip() if isinstance(x, str) else x

    @staticmethod
    def first_letter_acronym(text):
        if isinstance(text, str):
            parts = text.split(' ')
            processed = ''.join(word[0] if len(word) > 2 else word for word in parts)
            return processed
        else:
            return text

    @staticmethod
    def df_execute_bulk_db_load(df, table_name, engine):  # postgres_connection ):
        # cursor = postgres_connection.cursor()

        postgres_connection = engine.raw_connection()
        cursor = postgres_connection.cursor()
        try:
            # Create an empty buffer to store the csv data
            buffer = StringIO()
            # Write the dataframe to the buffer without the index column
            df.to_csv(buffer, index=False)
            # Reset the buffer position to the beginning
            buffer.seek(0)

            # Copy the data from the buffer to the database table using the csv format and header option.
            # Table must exist. Will overwrite that table.
            cursor.execute(f'COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER);', stream=buffer)

            # Commit the changes to the database
            postgres_connection.commit()
            buffer.close()

            logging.info(f"The following table was loaded successfully: {table_name}")

        except Exception as e:
            postgres_connection.rollback()
            logging.info(f"Error loading table: {table_name}\nError: {e}")
        finally:
            cursor.close()

    @staticmethod
    def execute_and_commit_queries(postgres_connection, queries):
        cursor = postgres_connection.cursor()

        try:
            for query in queries:
                try:
                    cursor.execute(query)
                    postgres_connection.commit()
                    logging.info(f"Query executed and committed successfully: {query}")

                except Exception as e:
                    postgres_connection.rollback()
                    logging.info(f"Error executing query: {query}\nError: {e}")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            cursor.close()

    @staticmethod
    def create_dataframe_without_duplicates(fields_to_distinct, dataframe):
        # Create a list of column expressions
        distinct_result = dataframe[fields_to_distinct].drop_duplicates(ignore_index=True)
        return distinct_result

    @staticmethod
    def validate_dataframe(dataframe, model):
        # Check if the DataFrame has the expected columns
        expected_columns = model.__annotations__.keys()
        missing_columns = set(expected_columns) - set(dataframe.columns)

        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

        # Additional validation rules can go here
        # For example, check for non-null values, data types, etc.

        return dataframe
