# Final Dim and Fact Queries
dim_batallion_sql_query = """
 INSERT INTO public.dim_battalion (battalion)
 SELECT battalion
 FROM stg_dim_battalion
 ON CONFLICT (battalion) DO NOTHING;
 """
dim_district_sql_query = """
 INSERT INTO public.dim_district(neighborhood_district, city,city_cleaned)
 SELECT neighborhood_district, city,city_cleaned
 FROM stg_dim_district
 ON CONFLICT (neighborhood_district) DO NOTHING;
 """

fact_fire_department_injuries_sql_query = """
 insert into fact_fire_department_injuries (incident_number, id, incident_date, dim_battalion_sk, dim_district_sk,
                                            suppression_units, suppression_personnel, ems_units, ems_personnel,
                                            other_units, other_personnel, estimated_property_loss,
                                            estimated_contents_loss, fire_fatalities, fire_injuries, civilian_fatalities,
                                            civilian_injuries, num_alarms)
 SELECT
   incident_number,
   id,
   incident_date,
   sdb.dim_battalion_sk,
   dd.dim_district_sk,
   suppression_units,
   suppression_personnel,
   ems_units,
   ems_personnel,
   other_units,
   other_personnel,
   estimated_property_loss,
   estimated_contents_loss,
   fire_fatalities,
   fire_injuries,
   civilian_fatalities,
   civilian_injuries,
   num_alarms
 FROM stg_fact_fire_department_injuries fact
 LEFT JOIN public.dim_battalion sdb on fact.battalion = sdb.battalion
 LEFT JOIN public.dim_district dd on fact.neighborhood_district = dd.neighborhood_district
 ON CONFLICT (id) DO NOTHING;
 """