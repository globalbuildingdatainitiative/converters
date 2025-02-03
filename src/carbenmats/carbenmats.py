import importlib.metadata
import json
import logging.config
from csv import DictReader
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from iso3166 import countries
import yaml
from lcax import (
    Project,
    LifeCycleStage,
    Location,
    Country,
    SoftwareInfo,
    ImpactCategoryKey,
    Unit,
    ProjectPhase,
    ProjectInfo1 as ProjectInfo,
    ValueUnit,
    AreaType,
    RoofType,
    BuildingType,
    BuildingTypology,
    GeneralEnergyClass,
)

logging.config.dictConfig(
    yaml.safe_load((Path(__file__).parent.parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)

MAPPING = json.loads((Path(__file__).parent / "mapping.json").read_text())

def get_roof_type(row: dict):
    data = row[MAPPING["project_info.roof_type"]]
    _keys = [key for key, value in MAPPING.items() if key.startswith("roof_type.") and value]

    for key in _keys:
        if data.lower() in MAPPING[key]:
            return RoofType[key.split(".")[1]]

    raise NotImplementedError(f"Unknown roof type: {data}")


def get_country(data: str) -> Country:
    country = countries.get(data)
    for _country in Country:
        if _country.value == country.alpha3.lower():
            return _country


def get_location(country: str, city: str | None):
    return Location(
        country=get_country(country),
        city=city,
    )


def get_name(name: str | None):
    if not name or name.lower() == "no data":
        return "Undefined"
    return name.strip()


def get_building_typology(row: dict):
    data = row[MAPPING["project_info.building_typology"]]

    building_typology_keys = [key for key, value in MAPPING.items() if key.startswith("building_typology.") and value]

    for key in building_typology_keys:
        if data.lower() in MAPPING[key]:
            return [BuildingTypology[key.split(".")[1]]]

    raise NotImplementedError(f"Unknown building typology: {data}")


def get_building_type(row: dict):
    data = row[MAPPING["project_info.building_type"]]
    building_type_keys = [key for key, value in MAPPING.items() if key.startswith("building_type.") and value]

    for key in building_type_keys:
        if data.lower() in MAPPING[key]:
            return BuildingType[key.split(".")[1]]

    raise NotImplementedError(f"Unknown building type: {data}")


def get_general_energy_class(row: dict):
    data = row[MAPPING["project_info.general_energy_class"]]
    _keys = [key for key, value in MAPPING.items() if key.startswith("general_energy_class.") and value]

    for key in _keys:
        if data.lower() in MAPPING[key]:
            return GeneralEnergyClass[key.split(".")[1]]

    raise NotImplementedError(f"Unknown general energy class: {data}")


def get_users(data: dict, key: str):
    data = get_value_or_none(data, key)
    if not data:
        return None
    return int(float(data))


def get_results(row: dict):
    results = {ImpactCategoryKey.gwp: {}}
    result_items = [(key, value) for key, value in MAPPING.items() if key.startswith("results.") and value]
    for key, value in result_items:
        if row[value]:
            results[ImpactCategoryKey[key.split(".")[1]]][LifeCycleStage[key.split(".")[2]]] = (
                    float(row[value]) * 50
            )

    return results if results[ImpactCategoryKey.gwp] else None


def get_year(row: dict, key: str):
    data = row[MAPPING[key]]
    try:
        return int(data)
    except ValueError:
        return None


def get_value_or_none(row: dict, key: str) -> str | int | float | None:
    _key = MAPPING[key]
    return row[_key] if row[_key] and row[_key].lower() != "no data" else None


def convert_row(row: dict):
    results = get_results(row)
    project = Project(
        assemblies={},
        reference_study_period=get_value_or_none(row, "reference_study_period"),
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        id=str(uuid5(NAMESPACE_URL, json.dumps(row))),
        impact_categories=[ImpactCategoryKey.gwp] if results else [],
        life_cycle_stages=list(
            results[ImpactCategoryKey.gwp].keys() if results else []
        ),
        location=get_location(
            get_value_or_none(row, "location.country"), get_value_or_none(row, "location.city")
        ),
        name=get_name(row[MAPPING["name"]]),
        project_info=ProjectInfo(
            type="buildingInfo",
            building_completion_year=get_value_or_none(row, "project_info.building_completion_year"),
            building_users=get_users(row, "project_info.building_users"),
            gross_floor_area=AreaType(
                unit=Unit.m2,
                value=get_value_or_none(row, "project_info.gross_floor_area.value") or 0,
                definition=get_value_or_none(row, "project_info.gross_floor_area.definition") or "",
            ),
            building_footprint=ValueUnit(unit=Unit.m2, value=get_value_or_none(row, "project_info.building_footprint.value")) if get_value_or_none(row, "project_info.building_footprint.value") else None,
            building_type=get_building_type(row),
            building_typology=get_building_typology(row),
            floors_above_ground=get_value_or_none(row, "project_info.floors_above_ground") or 0,
            floors_below_ground=get_value_or_none(row, "project_info.floors_below_ground") or None,
            frame_type=get_value_or_none(row, "project_info.frame_type"),
            general_energy_class=get_general_energy_class(row),
            roof_type=get_roof_type(row),
        ),
        meta_data={"assessment": {"year": get_year(row, "meta_data.assessment.year")}, "source": {"name": "CarbEnMats", "url": None}},
        project_phase=ProjectPhase.other,
        results=results,
        software_info=SoftwareInfo(
            lca_software=get_value_or_none(row, "software_info.lca_software") or "",
            goal_and_scope_definition=get_value_or_none(row, "software_info.goal_and_scope_definition"),
        ),
    )
    return project.model_dump(mode="json", by_alias=True)


def save_data(data: list[dict], data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Saving CarbEnMats to file: {file}")

    file.write_text(json.dumps(data, indent=2))


def load_carbenmats(data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Loading CarbEnMats from file: {file}")

    lcax_data = []
    with open(file) as _file:
        reader = DictReader(_file, delimiter=";")
        for index, row in enumerate(reader):
            log.debug(f"Processing row {index}")
            lcax_data.append(convert_row(row))

    save_data(lcax_data, data_folder, filename.replace(".csv", ".json"))


if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent.parent / "data"
    _filename = "carbenmats_0.2.0_full.csv"

    load_carbenmats(_data_folder, _filename)
    log.info("CarbEnMats loaded")
