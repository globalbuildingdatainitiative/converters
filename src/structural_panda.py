import json
from base64 import decode
from csv import DictReader
import importlib.metadata
from multiprocessing.managers import Value
from pathlib import Path
from lcax import Project, Assembly, LifeCycleStage, Location, Country, SoftwareInfo, ImpactCategoryKey, Unit, \
    ProjectPhase, ProjectInfo1 as ProjectInfo, ValueUnit, AreaType, RoofType, BuildingType, BuildingTypology, \
    GeneralEnergyClass, Product
from uuid import uuid4, uuid5, NAMESPACE_URL


def get_building_typology(_typo: str) -> list[BuildingTypology]:
    typology = []

    if 'office' in _typo.lower():
        typology.append(BuildingTypology.OFFICE)
    if 'residential' in _typo.lower():
        typology.append(BuildingTypology.RESIDENTIAL)
    if 'public' in _typo.lower():
        typology.append(BuildingTypology.PUBLIC)
    if 'commercial' in _typo.lower():
        typology.append(BuildingTypology.COMMERCIAL)
    if 'industrial' in _typo.lower():
        typology.append(BuildingTypology.INDUSTRIAL)
    if 'infrastructure' in _typo.lower():
        typology.append(BuildingTypology.INFRASTRUCTURE)
    if 'agricultural' in _typo.lower():
        typology.append(BuildingTypology.AGRICULTURAL)
    if _typo.lower() in ["educational", "healthcare"]:
        typology.append(BuildingTypology.PUBLIC)
    if _typo.lower() in ["other", "mixed use"]:
        typology.append(BuildingTypology.OTHER)
    if _typo.lower() == "science/lab":
        typology.append(BuildingTypology.INDUSTRIAL)

    if not typology:
        raise NotImplementedError(f"Unknown building typology: {_typo}")

    return typology


def get_building_type(_type: str) -> BuildingType:
    if _type in ["New Build (Brownfield)", "New Build (Greenfield)"]:
        return BuildingType.NEW_CONSTRUCTION_WORKS
    elif _type == "Mixed New Build/Refurb":
        return BuildingType.DECONSTRUCTION_AND_NEW_CONSTRUCTION_WORKS
    elif _type == "Full Refurb":
        return BuildingType.RETROFIT_WORKS
    else:
        raise NotImplementedError(f"Unknown building type: {_type}")


def convert_row(row: dict):
    results = {
        ImpactCategoryKey.GWP.value: {
            LifeCycleStage.A1A3.value: float(row["Carbon A1-A3 (kgCO2e)"]),
            LifeCycleStage.A4.value: float(row["Carbon A4 (kgCO2e)"]),
            LifeCycleStage.A5.value: float(row["Carbon A5a (kgCO2e)"]) + float(row["Carbon A5w (kgCO2e)"]),
            LifeCycleStage.B1.value: float(row["Carbon B1 (kgCO2e)"]),
            LifeCycleStage.C1.value: float(row["Carbon C1 (kgCO2e)"]),
            LifeCycleStage.C2.value: float(row["Carbon C2 (kgCO2e)"]),
            LifeCycleStage.D.value: float(row["Carbon D (kgCO2e)"])
        }
    }
    assembly = Assembly(
        id=str(uuid4()),
        name="Building",
        classification=None,
        products={},
        quantity=1,
        results=results,
        unit=Unit.PCS,
        type="actual"
    )
    project = Project(
        assemblies={assembly.id: assembly},
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        id=str(uuid5(NAMESPACE_URL, json.dumps(row))),
        impact_categories=[ImpactCategoryKey.GWP],
        life_cycle_stages=[LifeCycleStage.A1A3, LifeCycleStage.A4, LifeCycleStage.A5, LifeCycleStage.B1,
                           LifeCycleStage.C1, LifeCycleStage.C2, LifeCycleStage.D],
        location=Location(country=Country.GBR),
        name="Unknown",
        project_info=ProjectInfo(
            type="buildingInfo",
            gross_floor_area=AreaType(unit=Unit.M2, value=row["GIFA (Total)"], definition="GIFA"),
            building_footprint=ValueUnit(unit=Unit.M2, value=row["GIFA (Total)"]),
            building_type=get_building_type(row["Type"]),
            building_typology=get_building_typology(row["Project Sector"]),
            floors_above_ground=row["Storeys (#)"],
            heated_floor_area=AreaType(value=0, unit=Unit.M2, definition="Unknown"),
            frame_type=row["Superstructure Type"],
            general_energy_class=GeneralEnergyClass.UNKNOWN,
            roof_type=RoofType.OTHER
        ),
        meta_data={
            "assessment_year": row["Calculation Year"]
        },
        project_phase=ProjectPhase.OTHER,
        results=results,
        software_info=SoftwareInfo(lca_software="Structural Panda" if row["Used PANDA"] == "Yes" else "Unknown"),
    )
    return project.model_dump(mode='json', by_alias=True)


def save_data(data: list[dict], data_folder: Path, filename: str):
    file = data_folder / filename
    print(f"Saving structural panda to file: {file}")

    file.write_text(json.dumps(data, indent=2))


def load_structural_pands(data_folder: Path, filename: str):
    file = data_folder / filename
    print(f"Loading structural panda from file: {file}")

    lcax_data = []
    with open(file) as _file:
        reader = DictReader(_file)
        for row in reader:
            lcax_data.append(convert_row(row))

    save_data(lcax_data, data_folder, filename.replace(".csv", ".json"))


if __name__ == "__main__":
    _data_folder = (Path(__file__).parent.parent / "data")
    _filename = "structural_panda_20240716.csv"

    load_structural_pands(_data_folder, _filename)
    print("Structural panda loaded")
