class FieldSettings:
    __slots__ = (
        "raw_dim_district_fields",
        "raw_dim_battalion_fields",
        "stg_fact_fire_fighters_injured_fields",
        "dim_district_fields",
        "dim_battalion_fields"
    )

    def __init__(
            self,
    ):
        raw_dim_district_fields = ['neighborhood_district', 'city']
        raw_dim_battalion_fields = ['battalion']
        stg_fact_fire_fighters_injured_fields = ['incident_number', 'id', 'incident_date', 'battalion',
                                                 'neighborhood_district', 'suppression_units', 'suppression_personnel',
                                                 'ems_units', 'ems_personnel', 'other_units', 'other_personnel',
                                                 'estimated_property_loss', 'estimated_contents_loss',
                                                 'fire_fatalities', 'fire_injuries', 'civilian_fatalities',
                                                 'civilian_injuries', 'number_of_alarms'
                                                ]
        self.raw_dim_district_fields = raw_dim_district_fields
        self.raw_dim_battalion_fields = raw_dim_battalion_fields
        self.stg_fact_fire_fighters_injured_fields = stg_fact_fire_fighters_injured_fields
        self.dim_district_fields = Fields(raw_dim_district_fields)
        self.dim_battalion_fields = Fields(raw_dim_battalion_fields)


class Fields:
    def __init__(self, fields_list):
        self.fields_list = fields_list

    def __getattr__(self, name):
        if name in self.fields_list:
            return name
        raise AttributeError(f"Fields object has no attribute '{name}'")
