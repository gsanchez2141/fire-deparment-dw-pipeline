import logging
import sys
import boto3
from fire_department.setup.app_settings import AppSettings
from fire_department.setup.field_settings import FieldSettings
from fire_department.repository.postgres.gold import dim_batallion_sql_query, dim_district_sql_query, \
    fact_fire_department_injuries_sql_query
from fire_department.utils import Utils
from fire_department.repository.model.bronze import RawFireDepartmentModel, RawBattalionModel, RawDistrictModel
from fire_department.repository.model.silver import StgFireDepartmentModel, StgBattalionModel, StgDistrictModel
from sqlalchemy import create_engine

# Configure logging to send messages to CloudWatch Logs
logging.basicConfig(level=logging.INFO)


def validate_dataframes(dataframes_and_models):
    validated_dataframes = []
    for dataframe, model in dataframes_and_models:
        validated_dataframes.append(Utils.validate_dataframe(dataframe, model))
    return validated_dataframes

def execute_bulk_db_load(dataframes_and_table_names, engine):
    for dataframe, table_name in dataframes_and_table_names:
        Utils.df_execute_bulk_db_load(dataframe, table_name, engine)


def get_app_settings():
    return AppSettings()


def get_field_settings():
    return FieldSettings()


def main():
    logging.info('Fire Department Job Starting')

    app_settings = get_app_settings()
    fields = get_field_settings()

    # AWS
    region_name = app_settings.region_name
    local_stack_endpoint = app_settings.local_stack_endpoint
    bucket_name = app_settings.bucket_name

    # DB
    db_host = app_settings.db_host
    db_port = app_settings.db_port
    db_name = app_settings.db_name
    db_user = app_settings.db_user
    db_password = app_settings.db_password

    # Instantiate the S3 client
    s3_client = boto3.client('s3', region_name=region_name, endpoint_url=local_stack_endpoint)

    # Connections instantiations
    db_uri = f'postgresql+pg8000://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_uri)
    postgres_connection = Utils.connect_to_postgres(db_host, db_port, db_user, db_password, db_name)

    try:

        # List files in the S3 bucket
        file_names_info = Utils.list_files_in_s3(s3_client, bucket_name)

        # Get the file with the maximum last modified timestamp
        latest_file_name = Utils.get_latest_file_name(file_names_info)
        logging.info(f'Current file name: {latest_file_name}')

        # Retrieve the file from s3 in a pandas data frame
        raw_fact_fire_department = Utils.get_file_from_s3_as_dataframe(s3_client, bucket_name,
                                                                       latest_file_name,
                                                                       RawFireDepartmentModel)

        # Bronze STAGE
        # Raw dimensions creation
        raw_dim_district_dataframe = Utils.create_dataframe_without_duplicates(fields.raw_dim_district_fields,
                                                                               raw_fact_fire_department)
        raw_dim_battalion_dataframe = Utils.create_dataframe_without_duplicates(fields.raw_dim_battalion_fields,
                                                                                raw_fact_fire_department)
        # Validating raw/bronze data
        # List of dataframes and corresponding models
        raw_dataframes_and_models = [
            (raw_dim_battalion_dataframe, RawBattalionModel),
            (raw_dim_district_dataframe, RawDistrictModel),
            (raw_fact_fire_department, RawFireDepartmentModel),
        ]
        # Validating raw/bronze data
        raw_validated_dataframes = validate_dataframes(raw_dataframes_and_models)

        for validated_dataframe, table_name in zip(raw_validated_dataframes, [
            app_settings.raw_dim_battalion_table_name,
            app_settings.raw_dim_district_table_name,
            app_settings.raw_fact_fire_department_table_name,
        ]):
            # Writing raw/bronze data
            Utils.df_execute_bulk_db_load(validated_dataframe, table_name, engine)

        # Silver Stage
        # Stage/silver creation
        stg_dim_battalion_dataframe = (
            raw_dim_battalion_dataframe.map(Utils.clean_str)
        )
        stg_dim_district_dataframe = (
            raw_dim_district_dataframe.map(Utils.clean_str).assign(
                # creating a new normalized field for city
                city_cleaned=lambda df: df[fields.dim_district_fields.city].apply(Utils.first_letter_acronym))
        )
        stg_fact_fire_department = (raw_fact_fire_department[fields.stg_fact_fire_fighters_injured_fields].fillna(0)
        .assign(
            neighborhood_district=raw_fact_fire_department[fields.dim_district_fields.neighborhood_district].apply(
                Utils.clean_str),
            battalion=raw_fact_fire_department[fields.dim_battalion_fields.battalion].apply(
                Utils.clean_str)
        )
        )

        # List of dataframes and corresponding models
        stg_dataframes_and_models = [
            (stg_dim_battalion_dataframe, StgBattalionModel),
            (stg_dim_district_dataframe, StgDistrictModel),
            (stg_fact_fire_department, StgFireDepartmentModel),
        ]
        # Validating stg/silver data
        stg_validated_dataframes = validate_dataframes(stg_dataframes_and_models)

        for validated_dataframe, table_name in zip(stg_validated_dataframes, [
            app_settings.stg_dim_battalion_table_name,
            app_settings.stg_dim_district_table_name,
            app_settings.stg_fact_fire_department_table_name,
        ]):
            # Writing stg/silver data
            Utils.df_execute_bulk_db_load(validated_dataframe, table_name, engine)

        # GOLD Stage
        # Writing final/gold data
        Utils.execute_and_commit_queries(postgres_connection, [dim_batallion_sql_query
            , dim_district_sql_query
            , fact_fire_department_injuries_sql_query])
        logging.info(f"Dim and Fact query executed successfully")
        logging.info('The job has finished successfully!')

    except Exception as error:
        error_msg = f"{sys.exc_info()[0]}, {str(error)}"
        logging.error(error_msg, exc_info=True)
        raise Exception(error_msg) from error


if __name__ == "__main__":
    event_stream = None
    main()
