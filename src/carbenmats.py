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
    yaml.safe_load((Path(__file__).parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)


def get_roof_type(data: str):
    if data.lower() in ["flat roof"]:
        return RoofType.flat
    elif data.lower() in ["single pitched roof"]:
        return RoofType.pitched
    elif data.lower() in ["gable or saddle roof"]:
        return RoofType.saddle
    elif data.lower() in ["pyramid roof"]:
        return RoofType.pyramid
    elif data.lower() in ["no data", "other"]:
        return RoofType.other
    else:
        raise NotImplementedError(f"Unknown roof type: {data}")


def get_country(data: str) -> Country:
    country = countries.get(data)
    for _country in Country:
        if _country.value == country.alpha3.lower():
            return _country


def get_location(country: str, city: str):
    return Location(country=get_country(country), city=city if city and city.lower() != "no data" else None)


def get_name(name: str | None):
    if not name or name.lower() == "no data":
        return "Unknown"
    return name


def get_building_typology(data: str):
    if data.lower() in [
        "single family house",
        "multi-family house",
        "semi-detached",
        "row house",
    ]:
        return [BuildingTypology.residential]
    elif data.lower() in ["office"]:
        return [BuildingTypology.office]
    elif data.lower() in [
        "school and daycare",
        "hospital and health",
        "art & culture",
        "sport & entertainment",
    ]:
        return [BuildingTypology.public]
    elif data.lower() in ["hotel & resort", "retail and restaurant"]:
        return [BuildingTypology.commercial]
    elif data.lower() in ["aviation"]:
        return [BuildingTypology.infrastructure]
    elif data.lower() in ["technology & science"]:
        return [BuildingTypology.industrial]
    elif data.lower() in ["other", "mixed use", "no data"]:
        return [BuildingTypology.other]
    else:
        raise NotImplementedError(f"Unknown building typology: {data}")


def get_building_type(data: str):
    if data.lower() in ["new construction"]:
        return BuildingType.new_construction_works
    elif data.lower() in ["no data"]:
        return BuildingType.other
    elif data.lower() in ["refurbishment"]:
        return BuildingType.retrofit_works
    elif data.lower() in ["existing building"]:
        return BuildingType.operations
    else:
        raise NotImplementedError(f"Unknown building type: {data}")


def get_general_energy_class(data: str):
    if data.lower() in ["new standard"]:
        return GeneralEnergyClass.standard
    elif data.lower() in ["new advanced"]:
        return GeneralEnergyClass.advanced
    elif data.lower() in ["existing standard"]:
        return GeneralEnergyClass.existing
    elif data.lower() in ["no data"]:
        return GeneralEnergyClass.unknown
    else:
        raise NotImplementedError(f"Unknown energy class: {data}")


def get_users(data: str):
    if not data or data.lower() in ["no data"]:
        return None
    return int(float(data))


def get_results(data: dict):

    results = {ImpactCategoryKey.gwp: {}}
    if data["GHG_A123_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.a1a3] = float(data["GHG_A123_total"]) * 50
    if data["GHG_A45_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.a4] = float(data["GHG_A45_total"]) * 50
    if data["GHG_B1234_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.b1] = float(data["GHG_B1234_total"]) * 50
    if data["GHG_B67_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.b6] = float(data["GHG_B67_total"]) * 50
    if data["GHG_C12_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.c1] = float(data["GHG_C12_total"]) * 50
    if data["GHG_C34_total"]:
        results[ImpactCategoryKey.gwp][LifeCycleStage.c3] = float(data["GHG_C34_total"]) * 50
    return results if results[ImpactCategoryKey.gwp] else None

def convert_row(row: dict):
    results = get_results(row)
    project = Project(
        assemblies={},
        reference_study_period=row["lca_RSP"] if row["lca_RSP"] and row["lca_RSP"].lower() != "no data" else None,
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        id=str(uuid5(NAMESPACE_URL, json.dumps(row))),
        impact_categories=[ImpactCategoryKey.gwp] if results else [],
        life_cycle_stages=list(results[ImpactCategoryKey.gwp].keys() if results else []),
        location=get_location(
            row.get("site_country_iso2"), row.get("site_region_local")
        ),
        name=get_name(row.get("meta_title")),
        project_info=ProjectInfo(
            type="buildingInfo",
            building_completion_year=row["bldg_year_complete"] or None,
            building_users=get_users(row["bldg_users_total"]),
            gross_floor_area=AreaType(
                unit=Unit.m2,
                value=row["bldg_area_gfa"] or 0,
                definition=row["bldg_area_definition"],
            ),
            building_footprint=ValueUnit(unit=Unit.m2, value=row["bldg_footprint"])
            if row["bldg_footprint"]
            else None,
            building_type=get_building_type(row["bldg_project_type"]),
            building_typology=get_building_typology(row["bldg_use_subtype"]),
            floors_above_ground=row["bldg_floors_ag"] or 0,
            floors_below_ground=row.get("bldg_floors_bg", None) or None,
            # heated_floor_area=AreaType(value=0, unit=Unit.m2, definition="Unknown"),
            frame_type=row["bldg_struct_type"],
            general_energy_class=get_general_energy_class(
                row["bldg_energy_class_general"]
            ),
            roof_type=get_roof_type(row["bldg_roof_type"]),
        ),
        meta_data={"assessment_year": row["meta_year"]},
        project_phase=ProjectPhase.other,
        results=results,
        software_info=SoftwareInfo(
            lca_software=row["lca_software"],
            goal_and_scope_definition=row["lca_goal_scope"],
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
    _data_folder = Path(__file__).parent.parent / "data"
    _filename = "carbenmats_0.2.0_full.csv"

    load_carbenmats(_data_folder, _filename)
    log.info("CarbEnMats loaded")
