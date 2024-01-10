from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StgFireDepartmentModel():
    incident_number: Optional[int]
    id: Optional[int]
    incident_date: Optional[datetime]
    battalion: Optional[str]
    neighborhood_district: Optional[str]
    suppression_units: Optional[int]
    suppression_personnel: Optional[int]
    ems_units: Optional[int]
    ems_personnel: Optional[int]
    other_units: Optional[int]
    other_personnel: Optional[int]
    estimated_property_loss: Optional[float]
    estimated_contents_loss: Optional[float]
    fire_fatalities: Optional[int]
    fire_injuries: Optional[int]
    civilian_fatalities: Optional[int]
    civilian_injuries: Optional[int]
    number_of_alarms: Optional[int]


@dataclass
class StgDistrictModel:
    neighborhood_district: Optional[str]
    city: Optional[str]


@dataclass
class StgBattalionModel:
    battalion: Optional[str]
