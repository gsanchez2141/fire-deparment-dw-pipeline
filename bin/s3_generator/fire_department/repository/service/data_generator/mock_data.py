from faker import Faker
from shapely.geometry import Point
from shapely.wkt import dumps as wkt_dumps
import random
from typing import NamedTuple


fake = Faker()
def generate_sample_payload_enhanced(num_new_rows: int, max_values: NamedTuple) -> dict:
    # Use the retrieved max values for generating new sample payload
    max_incident_number, max_id_value, max_call_number = max_values

    incident_number = fake.random_int(max_incident_number + 1, max_incident_number + num_new_rows)
    exposure_number = fake.random_int(0, 1)
    id_value = fake.random_int(max_id_value + 1, max_id_value + num_new_rows)
    address = fake.street_address()
    incident_date = (
            fake.date_time_this_decade() + fake.time_delta()).isoformat()  # fake.date_this_decade().isoformat()
    call_number = fake.random_int(max_call_number + 1, max_call_number + num_new_rows)
    alarm_datetime = fake.date_time_this_decade().isoformat()
    arrival_datetime = (fake.date_time_this_decade() + fake.time_delta()).isoformat()
    close_datetime = (fake.date_time_this_decade() + fake.time_delta()).isoformat()
    city = 'SF'
    zipcode = fake.zipcode()
    battalion = fake.random_element(elements=('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10'))
    station_area = fake.word()
    box = fake.word()
    suppression_units = fake.random_int(1, 10)
    suppression_personnel = fake.random_int(1, 50)
    ems_units = fake.random_int(0, 5)
    ems_personnel = fake.random_int(0, 20)
    other_units = fake.random_int(0, 5)
    other_personnel = fake.random_int(0, 20)
    first_unit_on_scene = fake.random_element(elements=('E01', 'E02', 'T01', 'B01', 'MED01'))
    estimated_property_loss = random.uniform(0, 50000)
    estimated_contents_loss = random.uniform(0, 50000)
    fire_fatalities = fake.random_int(0, 2)
    fire_injuries = fake.random_int(0, 5)
    civilian_fatalities = fake.random_int(0, 2)
    civilian_injuries = fake.random_int(0, 5)
    num_alarms = 1
    primary_situation = fake.random_element(elements=('412 - Gas leak (natural gas or LPG)',
                                                      '552 - Police matter',
                                                      '210 - Steam Rupture, steam, other',
                                                      '522 - Water or steam leak',
                                                      '520 - Water problem, other',
                                                      '733 - Smoke detector activation/malfunction',
                                                      '711 - Municipal alarm system, Street Box False'))
    mutual_aid = 'None'
    action_taken_primary_scenarios = [
        '86 - Investigate',
        '71 - Extinguish',
        '45 - Rescue, remove from harm',
        '32 - Provide first aid & check for injuries',
        '22 - Search & rescue, other',
        '94 - Disregard',
        '12 - Fire control or extinguishment, other',
        '58 - Assist physically disabled',
        '67 - Victim impaled on object',
        '39 - Extricate victim(s) from stalled elevator',
    ]
    action_taken_primary = fake.random_element(elements=action_taken_primary_scenarios)
    action_taken_secondary = '-'
    action_taken_other = '-'
    detector_alerted_occupants = '-'
    property_use = fake.random_element(elements=('962 - Residential street, road or residential dr',
                                                 '960 - Street, other',
                                                 '429 - Multifamily dwellings',
                                                 '400 - Residential, other'))
    area_of_fire_origin = '-'
    ignition_cause = '-'
    ignition_factor_primary = '-'
    ignition_factor_secondary = '-'
    heat_source = '-'
    item_first_ignited = '-'
    human_factors_associated_with_ignition = '-'
    structure_type = '-'
    structure_status = '-'
    floor_of_fire_origin = '-'
    fire_spread = '-'
    no_flame_spread = '-'
    num_floors_with_min_damage = 0
    num_floors_with_significant_damage = 0
    num_floors_with_heavy_damage = 0
    num_floors_with_extreme_damage = 0
    detectors_present = '-'
    detector_type = '-'
    detector_operation = '-'
    detector_effectiveness = '-'
    detector_failure_reason = '-'
    automatic_extinguishing_system_present = '-'
    automatic_extinguishing_system_type = '-'
    automatic_extinguishing_system_performance = '-'
    automatic_extinguishing_system_failure_reason = '-'
    num_sprinkler_heads_operating = 0
    supervisor_district = fake.random_int(1, 11)
    neighborhood_district = fake.random_element(elements=('Financial District/South Beach', 'Outer Richmond',
                                                          'Hayes Valley', 'South of Market', 'Potrero Hill',
                                                          'Bernal Heights', 'Inner Sunset'))
    point = generate_random_point()  # wkt_dumps(Point(fake.longitude(), fake.latitude()))

    return {
        "incident_number": incident_number,
        "exposure_number": exposure_number,
        "id": id_value,
        "address": address,
        "incident_date": incident_date,
        "call_number": call_number,
        "alarm_datetime": alarm_datetime,
        "arrival_datetime": arrival_datetime,
        "close_datetime": close_datetime,
        "city": city,
        "zipcode": zipcode,
        "battalion": battalion,
        "station_area": station_area,
        "box": box,
        "suppression_units": suppression_units,
        "suppression_personnel": suppression_personnel,
        "ems_units": ems_units,
        "ems_personnel": ems_personnel,
        "other_units": other_units,
        "other_personnel": other_personnel,
        "first_unit_on_scene": first_unit_on_scene,
        "estimated_property_loss": estimated_property_loss,
        "estimated_contents_loss": estimated_contents_loss,
        "fire_fatalities": fire_fatalities,
        "fire_injuries": fire_injuries,
        "civilian_fatalities": civilian_fatalities,
        "civilian_injuries": civilian_injuries,
        "num_alarms": num_alarms,
        "primary_situation": primary_situation,
        "mutual_aid": mutual_aid,
        "action_taken_primary": action_taken_primary,
        "action_taken_secondary": action_taken_secondary,
        "action_taken_other": action_taken_other,
        "detector_alerted_occupants": detector_alerted_occupants,
        "property_use": property_use,
        "area_of_fire_origin": area_of_fire_origin,
        "ignition_cause": ignition_cause,
        "ignition_factor_primary": ignition_factor_primary,
        "ignition_factor_secondary": ignition_factor_secondary,
        "heat_source": heat_source,
        "item_first_ignited": item_first_ignited,
        "human_factors_associated_with_ignition": human_factors_associated_with_ignition,
        "structure_type": structure_type,
        "structure_status": structure_status,
        "floor_of_fire_origin": floor_of_fire_origin,
        "fire_spread": fire_spread,
        "no_flame_spread": no_flame_spread,
        "num_floors_with_min_damage": num_floors_with_min_damage,
        "num_floors_with_significant_damage": num_floors_with_significant_damage,
        "num_floors_with_heavy_damage": num_floors_with_heavy_damage,
        "num_floors_with_extreme_damage": num_floors_with_extreme_damage,
        "detectors_present": detectors_present,
        "detector_type": detector_type,
        "detector_operation": detector_operation,
        "detector_effectiveness": detector_effectiveness,
        "detector_failure_reason": detector_failure_reason,
        "automatic_extinguishing_system_present": automatic_extinguishing_system_present,
        "automatic_extinguishing_system_type": automatic_extinguishing_system_type,
        "automatic_extinguishing_system_performance": automatic_extinguishing_system_performance,
        "automatic_extinguishing_system_failure_reason": automatic_extinguishing_system_failure_reason,
        "num_sprinkler_heads_operating": num_sprinkler_heads_operating,
        "supervisor_district": supervisor_district,
        "neighborhood_district": neighborhood_district,
        "point": point
    }


def generate_random_point() -> str:
    # Generate random coordinates within a specific bounding box
    min_longitude, max_longitude = -180, 180
    min_latitude, max_latitude = -90, 90

    longitude = random.uniform(min_longitude, max_longitude)
    latitude = random.uniform(min_latitude, max_latitude)

    # Create a Shapely Point object and convert it to WKT format
    point = Point(longitude, latitude)
    return wkt_dumps(point)
