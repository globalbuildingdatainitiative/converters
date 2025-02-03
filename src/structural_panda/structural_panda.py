import importlib.metadata
import json
import logging.config
from csv import DictReader
from pathlib import Path
from uuid import uuid5, NAMESPACE_URL

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


def get_building_typology(row: dict):
    data = row[MAPPING["project_info.building_typology"]]

    building_typology_keys = [
        key
        for key, value in MAPPING.items()
        if key.startswith("building_typology.") and value
    ]
    building_typology = []
    for key in building_typology_keys:
        if data.lower() in MAPPING[key]:
            building_typology.append(BuildingTypology[key.split(".")[1]])

    if building_typology:
        return building_typology
    raise NotImplementedError(f"Unknown building typology: {data}")


def get_building_type(row: dict):
    data = row[MAPPING["project_info.building_type"]]
    building_type_keys = [
        key
        for key, value in MAPPING.items()
        if key.startswith("building_type.") and value
    ]

    for key in building_type_keys:
        if data.lower() in MAPPING[key]:
            return BuildingType[key.split(".")[1]]

    raise NotImplementedError(f"Unknown building type: {data}")


def get_results(row: dict):
    results = {ImpactCategoryKey.gwp: {}}
    result_items = [
        (key, value)
        for key, value in MAPPING.items()
        if key.startswith("results.") and value
    ]
    for key, value in result_items:
        if key == "results.gwp.a5":
            results[ImpactCategoryKey[key.split(".")[1]]][
                LifeCycleStage[key.split(".")[2]]
            ] = float(row[value[0]]) + float(row[value[1]])
        elif row[value]:
            results[ImpactCategoryKey[key.split(".")[1]]][
                LifeCycleStage[key.split(".")[2]]
            ] = float(row[value])

    return results if results[ImpactCategoryKey.gwp] else None


def convert_row(row: dict):
    results = get_results(row)
    project = Project(
        assemblies={},
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        id=str(uuid5(NAMESPACE_URL, json.dumps(row))),
        impact_categories=list(results.keys()) if results else [],
        life_cycle_stages=list(results[ImpactCategoryKey.gwp].keys())
        if results
        else [],
        location=Location(country=Country.gbr),
        name="Undefined",
        project_info=ProjectInfo(
            type="buildingInfo",
            gross_floor_area=AreaType(
                unit=Unit.m2,
                value=row[MAPPING["project_info.gross_floor_area.value"]],
                definition="GIFA",
            ),
            building_type=get_building_type(row),
            building_typology=get_building_typology(row),
            floors_above_ground=row[MAPPING["project_info.floors_above_ground"]],
            frame_type=row[MAPPING["project_info.frame_type"]],
            general_energy_class=GeneralEnergyClass.unknown,
            roof_type=RoofType.other,
        ),
        meta_data={
            "assessment": {"year": row[MAPPING["meta_data.assessment.year"]]},
            "source": {"name": "StructuralPanda", "url": None},
        },
        project_phase=ProjectPhase.other,
        results=results,
        software_info=SoftwareInfo(
            lca_software="Structural Panda" if row["Used PANDA"] == "Yes" else ""
        ),
    )
    return project.model_dump(mode="json", by_alias=True)


def save_data(data: list[dict], data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Saving structural panda to file: {file}")

    file.write_text(json.dumps(data, indent=2))


def load_structural_pands(data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Loading structural panda from file: {file}")

    lcax_data = []
    with open(file) as _file:
        reader = DictReader(_file)
        for index, row in enumerate(reader):
            log.debug(f"Processing row {index}")
            lcax_data.append(convert_row(row))

    save_data(lcax_data, data_folder, filename.replace(".csv", ".json"))


if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent.parent / "data"
    _filename = "structural_panda_20240716.csv"

    load_structural_pands(_data_folder, _filename)
    log.info("Structural panda loaded")
