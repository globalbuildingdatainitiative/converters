import importlib.metadata
import json
from csv import DictReader
from pathlib import Path
from uuid import uuid4, uuid5, NAMESPACE_URL
import logging.config
import yaml

from lcax import (
    Project,
    Assembly,
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


def get_building_typology(_typo: str) -> list[BuildingTypology]:
    typology = []

    if "office" in _typo.lower():
        typology.append(BuildingTypology.office)
    if "residential" in _typo.lower():
        typology.append(BuildingTypology.residential)
    if "public" in _typo.lower():
        typology.append(BuildingTypology.public)
    if "commercial" in _typo.lower():
        typology.append(BuildingTypology.commercial)
    if "industrial" in _typo.lower():
        typology.append(BuildingTypology.industrial)
    if "infrastructure" in _typo.lower():
        typology.append(BuildingTypology.infrastructure)
    if "agricultural" in _typo.lower():
        typology.append(BuildingTypology.agricultural)
    if _typo.lower() in ["educational", "healthcare"]:
        typology.append(BuildingTypology.public)
    if _typo.lower() in ["other", "mixed use"]:
        typology.append(BuildingTypology.other)
    if _typo.lower() == "science/lab":
        typology.append(BuildingTypology.industrial)

    if not typology:
        raise NotImplementedError(f"Unknown building typology: {_typo}")

    return typology


def get_building_type(_type: str) -> BuildingType:
    if _type in ["New Build (Brownfield)", "New Build (Greenfield)"]:
        return BuildingType.new_construction_works
    elif _type == "Mixed New Build/Refurb":
        return BuildingType.deconstruction_and_new_construction_works
    elif _type == "Full Refurb":
        return BuildingType.retrofit_works
    else:
        raise NotImplementedError(f"Unknown building type: {_type}")


def convert_row(row: dict):
    results = {
        ImpactCategoryKey.gwp.value: {
            LifeCycleStage.a1a3.value: float(row["Carbon A1-A3 (kgCO2e)"]),
            LifeCycleStage.a4.value: float(row["Carbon A4 (kgCO2e)"]),
            LifeCycleStage.a5.value: float(row["Carbon A5a (kgCO2e)"])
            + float(row["Carbon A5w (kgCO2e)"]),
            LifeCycleStage.b1.value: float(row["Carbon B1 (kgCO2e)"]),
            LifeCycleStage.c1.value: float(row["Carbon C1 (kgCO2e)"]),
            LifeCycleStage.c2.value: float(row["Carbon C2 (kgCO2e)"]),
            LifeCycleStage.d.value: float(row["Carbon D (kgCO2e)"]),
        }
    }
    assembly = Assembly(
        id=str(uuid4()),
        name="Building",
        classification=None,
        products={},
        quantity=1,
        results=results,
        unit=Unit.pcs,
        type="actual",
    )
    project = Project(
        assemblies={assembly.id: assembly},
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        id=str(uuid5(NAMESPACE_URL, json.dumps(row))),
        impact_categories=[ImpactCategoryKey.gwp],
        life_cycle_stages=[
            LifeCycleStage.a1a3,
            LifeCycleStage.a4,
            LifeCycleStage.a5,
            LifeCycleStage.b1,
            LifeCycleStage.c1,
            LifeCycleStage.c2,
            LifeCycleStage.d,
        ],
        location=Location(country=Country.gbr),
        name="Undefined",
        project_info=ProjectInfo(
            type="buildingInfo",
            gross_floor_area=AreaType(
                unit=Unit.m2, value=row["GIFA (Total)"], definition="GIFA"
            ),
            building_footprint=ValueUnit(unit=Unit.m2, value=row["GIFA (Total)"]),
            building_type=get_building_type(row["Type"]),
            building_typology=get_building_typology(row["Project Sector"]),
            floors_above_ground=row["Storeys (#)"],
            heated_floor_area=AreaType(value=0, unit=Unit.m2, definition="Unknown"),
            frame_type=row["Superstructure Type"],
            general_energy_class=GeneralEnergyClass.unknown,
            roof_type=RoofType.other,
        ),
        meta_data={"assessment": { "year": row["Calculation Year"]}, "source": { "name": "StructuralPanda", "url": None}},
        project_phase=ProjectPhase.other,
        results=results,
        software_info=SoftwareInfo(
            lca_software="Structural Panda" if row["Used PANDA"] == "Yes" else "Unknown"
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
    _data_folder = Path(__file__).parent.parent / "data"
    _filename = "structural_panda_20240716.csv"

    load_structural_pands(_data_folder, _filename)
    log.info("Structural panda loaded")
