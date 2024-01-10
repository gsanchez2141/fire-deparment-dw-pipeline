import sys
import boto3
import logging
import polars as pl
from datetime import datetime
from typing import List, NamedTuple, Optional

from fire_department.setup.app_settings import AppSettings
from fire_department.utils import Utils
from fire_department.repository.model.fire_department_model import fire_department_schema
from fire_department.repository.service.data_generator.mock_data import generate_sample_payload_enhanced

# Configure logging to send messages to CloudWatch Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_app_settings():
    return AppSettings()


def get_max_values_from_csv(data_dataframe: pl.DataFrame, fields: List[str]) -> NamedTuple:
    # Create a named tuple dynamically based on the field names
    MaxValues: NamedTuple = NamedTuple('MaxValues', [(field, int) for field in fields])

    # Initialize max values dictionary
    max_values = {}

    # Retrieve the max values for each field
    for field in fields:
        max_values[field] = data_dataframe[field].max()

    # Extract values for all fields dynamically
    max_values_tuple = MaxValues(**{field: max_values.get(field, 0) for field in fields})

    return max_values_tuple


def generate_mocked_data_as_dataframe(num_new_rows: int, max_values: NamedTuple) -> pl.DataFrame:
    # Create a list of dictionaries using list comprehension with mocked data
    mocked_data = [generate_sample_payload_enhanced(num_new_rows, max_values) for _ in range(num_new_rows)]

    # # Load the list of dictionaries into a Polars DataFrame
    df = pl.DataFrame(mocked_data)
    df = (df
          .with_columns(pl.col('incident_date').cast(pl.Datetime)
                        , pl.col('alarm_dttm').cast(pl.Datetime)
                        , pl.col('arrival_dttm').cast(pl.Datetime)
                        , pl.col('close_dttm').cast(pl.Datetime))
          )  # For DateTime

    return df


def remove_duplicates_from_dataframe(dataframe: pl.DataFrame, fields: List[str]) -> pl.DataFrame:
    # List to store conditions for each field
    conditions = []

    for field in fields:
        # Count occurrences of each value in the field and create a new DataFrame
        value_counts = (dataframe
                        .group_by(field)
                        .agg(pl.count().alias(f'{field}_count'))
                        )

        # Filter values that have count greater than 1 (repeated)
        repeated_values = (value_counts
                           .filter(pl.col(f'{field}_count') > 1)
                           .select(field, f'{field}_count')
                           )

        # Create a condition for the field where the value is in the set of repeated values
        condition = ~dataframe[field].is_in(repeated_values[field])

        # Append the condition to the list
        conditions.append(condition)

    # Combine conditions using logical AND
    final_condition = conditions[0]
    for condition in conditions[1:]:
        final_condition = final_condition & condition

    # Apply the combined condition to the original DataFrame
    dataframe_cleaned = dataframe.filter(final_condition)

    return dataframe_cleaned


def find_repeated_values_by_field(dataframe: pl.DataFrame, fields: List[str]) -> pl.DataFrame:
    # Validation layer
    repeated_values_dfs = []

    for field in fields:
        # Count occurrences of each value in the field and create a new DataFrame
        value_counts = (dataframe
                        .group_by(field)
                        .agg(pl.count().alias(f'{field}_count'))
                        )

        # Filter values that have count greater than 1 (repeated)
        repeated_values = (value_counts.
                           filter(pl.col(f'{field}_count') > 1)
                           .select(field, f'{field}_count')
                           )

        # Sort the DataFrame by count in descending order
        repeated_values = (repeated_values.
                           sort(f'{field}_count', descending=True)
                           )

        # Append to the list of DataFrames
        repeated_values_dfs.append(repeated_values)

    # Concatenate all DataFrames into a single result DataFrame
    repeated_values_df = pl.concat(repeated_values_dfs, how="horizontal")

    return repeated_values_df


def get_new_latest_file_name(latest_file_name: Optional[str]) -> str:
    # Add the timestamp suffix to the existing filename (if it exists) ensuring it ends with ".csv" or create a new
    # one if no existing file
    # Generate a timestamp for the filename suffix
    timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Remove everything after the second underscore and concatenate timestamp_suffix
    if latest_file_name:
        base_filename, extension = latest_file_name.split('_', 2)[:2]
        filename = f'{base_filename}_{extension}_{timestamp_suffix}.csv'
    else:
        filename = f'combined_data_{timestamp_suffix}.csv'

    return filename


def main() -> None:
    num_new_rows = 10000  # This number includes duplicates, these are removed on a following step - To be improved

    app_settings = get_app_settings()
    bucket_name = app_settings.bucket_name

    fields_to_check_duplicates = app_settings.fields_to_check_duplicates
    s3_client = boto3.client('s3', region_name=app_settings.region_name, endpoint_url=app_settings.local_stack_endpoint)

    # List files in the S3 bucket
    file_names_info = Utils.list_files_in_s3(s3_client, bucket_name)

    # Get the file with the maximum last modified timestamp
    latest_file_name = Utils.get_latest_file_name(file_names_info)
    if latest_file_name is None:
        logging.warning('No file found on s3')
        sys.exit()

    # Retrieve the file from s3 in a polars data frame
    latest_s3_file_dataframe = Utils.get_file_from_s3_as_dataframe(s3_client, bucket_name, latest_file_name,
                                                                   fire_department_schema)
    # Get the max values from the CSV file in s3 according to fields_to_check_duplicates
    max_values = get_max_values_from_csv(latest_s3_file_dataframe, fields_to_check_duplicates)

    # Generate mocked dataframe with new values
    # try passing a schema definition
    mocked_data_dataframe = generate_mocked_data_as_dataframe(num_new_rows, max_values)

    # Clean generated mocked dataframe
    mocked_data_dataframe_cleaned = remove_duplicates_from_dataframe(mocked_data_dataframe, fields_to_check_duplicates)

    # Validation layer
    repeated_values = find_repeated_values_by_field(mocked_data_dataframe_cleaned, fields_to_check_duplicates)
    if repeated_values.shape[0] > 0:
        Utils.check_and_log_error(repeated_values)

    # Append new DataFrame to the existing DataFrame
    combined_dataframe = pl.concat([latest_s3_file_dataframe, mocked_data_dataframe_cleaned])

    # New latest_file_name
    new_latest_file_name = get_new_latest_file_name(latest_file_name)

    # Send messages
    Utils.send_csv_to_s3(s3_client, bucket_name, new_latest_file_name, combined_dataframe)
    logger.info(f"Uploaded {len(combined_dataframe)} rows in a csv file to S3 as {new_latest_file_name}")
    logger.info(f'New rows: {len(mocked_data_dataframe_cleaned)}')
    logger.info("Finished processing new mocked rows")


if __name__ == "__main__":
    main()
