class AppSettings:
    __slots__ = [
        "raw_dim_battalion_table_name",
        "raw_dim_district_table_name",
        "raw_fact_fire_department_table_name",
        "stg_dim_battalion_table_name",
        "stg_dim_district_table_name",
        "stg_fact_fire_department_table_name",
        "bucket_name",
        "region_name",
        "local_stack_endpoint",
        "db_host",
        "db_port",
        "db_name",
        "db_user",
        "db_password"

    ]

    def __init__(
            self,
    ):
        self.raw_dim_battalion_table_name = "raw_dim_battalion"
        self.raw_dim_district_table_name = "raw_dim_district"
        self.raw_fact_fire_department_table_name = "raw_fact_fire_department"
        self.stg_dim_battalion_table_name = "stg_dim_battalion"
        self.stg_dim_district_table_name = "stg_dim_district"
        self.stg_fact_fire_department_table_name = "stg_fact_fire_department_injuries"
        self.bucket_name = "fire-department"
        self.region_name = "us-east-1"
        self.local_stack_endpoint = "http://localhost:4566"
        self.db_host = "localhost"
        self.db_port = "5432"
        self.db_name = "mydatabase"
        self.db_user = "myuser"
        self.db_password = "mypassword"



