-- init.sql
CREATE EXTENSION IF NOT EXISTS postgis;

-- Generic dim_date postgres table
-- https://duffn.medium.com/creating-a-date-dimension-table-in-postgresql-af3f8e2941ac

DROP TABLE if exists dim_date;

CREATE TABLE dim_date
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE public.dim_date ADD CONSTRAINT dim_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX dim_date_date_actual_idx
  ON dim_date(date_actual);

COMMIT;

INSERT INTO dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;

drop table if exists public.dim_battalion;
create table public.dim_battalion
(
    dim_battalion_SK BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    battalion TEXT UNIQUE
);

drop table if exists public.dim_district;
create table public.dim_district
(
    dim_district_SK BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    neighborhood_district TEXT UNIQUE,
    city text,
    city_cleaned text
);

drop table if exists public.fact_fire_department_injuries;
create table public.fact_fire_department_injuries
(
    incident_number         bigint ,
    id                      bigint PRIMARY KEY,
    incident_date           timestamp,
    dim_battalion_sk        bigint,
    dim_district_sk         bigint,
    suppression_units       bigint,
    suppression_personnel   bigint,
    ems_units               bigint,
    ems_personnel           bigint,
    other_units             bigint,
    other_personnel         bigint,
    estimated_property_loss double precision,
    estimated_contents_loss double precision,
    fire_fatalities         bigint,
    fire_injuries           bigint,
    civilian_fatalities     bigint,
    civilian_injuries       bigint,
    num_alarms              bigint
);

drop table if exists public.raw_dim_district;
create table public.raw_dim_district
(
    neighborhood_district text,
    city                  text
);

drop table if exists public.raw_dim_battalion;
create table public.raw_dim_battalion
(
    battalion text
);

drop table if exists public.raw_fact_fire_department;
create table public.raw_fact_fire_department
(
    incident_number                               bigint,
    exposure_number                               bigint,
    id                                            bigint,
    address                                       text,
    incident_date                                 timestamp,
    call_number                                   bigint,
    alarm_datetime                                timestamp,
    arrival_datetime                              timestamp,
    close_datetime                                timestamp,
    city                                          text,
    zipcode                                       text,
    battalion                                     text,
    station_area                                  text,
    box                                           text,
    suppression_units                             bigint,
    suppression_personnel                         bigint,
    ems_units                                     bigint,
    ems_personnel                                 bigint,
    other_units                                   bigint,
    other_personnel                               bigint,
    first_unit_on_scene                           text,
    estimated_property_loss                       double precision,
    estimated_contents_loss                       double precision,
    fire_fatalities                               bigint,
    fire_injuries                                 bigint,
    civilian_fatalities                           bigint,
    civilian_injuries                             bigint,
    num_alarms                                    bigint,
    primary_situation                             text,
    mutual_aid                                    text,
    action_taken_primary                          text,
    action_taken_secondary                        text,
    action_taken_other                            text,
    detector_alerted_occupants                    text,
    property_use                                  text,
    area_of_fire_origin                           text,
    ignition_cause                                text,
    ignition_factor_primary                       text,
    ignition_factor_secondary                     text,
    heat_source                                   text,
    item_first_ignited                            text,
    human_factors_associated_with_ignition        text,
    structure_type                                text,
    structure_status                              text,
    floor_of_fire_origin                          text,
    fire_spread                                   text,
    no_flame_spread                               text,
    num_floors_with_min_damage                    double precision,
    num_floors_with_significant_damage            double precision,
    num_floors_with_heavy_damage                  double precision,
    num_floors_with_extreme_damage                double precision,
    detectors_present                             text,
    detector_type                                 text,
    detector_operation                            text,
    detector_effectiveness                        text,
    detector_failure_reason                       text,
    automatic_extinguishing_system_present        text,
    automatic_extinguishing_system_type           text,
    automatic_extinguishing_system_performance    text,
    automatic_extinguishing_system_failure_reason text,
    num_sprinkler_heads_operating                 double precision,
    supervisor_district                           double precision,
    neighborhood_district                         text,
    point                                         text
);

drop table if exists public.stg_dim_battalion;
create table public.stg_dim_battalion
(
    battalion text
);

drop table if exists public.stg_dim_district;
create table public.stg_dim_district
(
    neighborhood_district text,
    city                  text,
    city_cleaned          text
);

drop table if exists public.stg_fact_fire_department_injuries;
create table public.stg_fact_fire_department_injuries
(
    incident_number         bigint,
    id                      bigint,
    incident_date           timestamp,
    battalion               text,
    neighborhood_district   text,
    suppression_units       bigint,
    suppression_personnel   bigint,
    ems_units               bigint,
    ems_personnel           bigint,
    other_units             bigint,
    other_personnel         bigint,
    estimated_property_loss double precision,
    estimated_contents_loss double precision,
    fire_fatalities         bigint,
    fire_injuries           bigint,
    civilian_fatalities     bigint,
    civilian_injuries       bigint,
    num_alarms              bigint
);

