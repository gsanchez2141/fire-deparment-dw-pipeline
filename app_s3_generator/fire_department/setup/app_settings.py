class AppSettings:
    __slots__ = [
        "bucket_name",
        "region_name",
        "local_stack_endpoint",
        "fields_to_check_duplicates"
    ]

    def __init__(
            self,
    ):
        self.bucket_name = "fire-department"
        self.region_name = "us-east-1"
        self.local_stack_endpoint = "http://localhost:4566"
        self.fields_to_check_duplicates = ['incident_number', 'id', 'call_number']
