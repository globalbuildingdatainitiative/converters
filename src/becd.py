import importlib.metadata
import json
import logging.config
from csv import DictReader
from datetime import datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from iso3166 import countries
import yaml
from lcax import (
    Project,
    Product,
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
    ReferenceSourceForImpactDataSource2 as TechFlow,
    BuildingType,
    BuildingTypology,
    Assembly,
    GeneralEnergyClass, calculate_project,
)


logging.config.dictConfig(
    yaml.safe_load((Path(__file__).parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)


def date_to_year(date: str):
    if date:
        return datetime.strptime(date, '%d/%m/%Y %H:%M:%S').year
    else:
        return None


def get_building_type(data: str):
    if data.lower() in ["new built"]:
        return BuildingType.new_construction_works
    else:
        raise NotImplementedError(f"Unknown building type: {data}")


def get_country(data: str) -> Country:
    try:
        country = countries.get(data)
    except KeyError:
        return Country.unknown
    for _country in Country:
        if _country.value == country.alpha3.lower():
            return _country


def get_location(city: str, country: str):
    return Location(
        country=get_country(country),
        city=city if city and city.lower() != "no data" else None,
    )


def update_project(project, row):
    if row["EmissionsIncluded"] == "No":
        return project
    product = Product(
        id=str(uuid5(NAMESPACE_URL, row["EntityElementName"])),
        name=row["EntityElementName"],
        description="",
        impact_data=TechFlow(
            id=str(uuid5(NAMESPACE_URL, row["EntityElementName"])),
            name=row["EntityElementName"],
            declared_unit=Unit.kg,
            format_version=importlib.metadata.version("lcax"),
            source=None,
            comment=None,
            location=Country.unknown,
            conversions=None,
            impacts=get_assembly_results(row),
            meta_data=None,
            type="actual",
        ),
        quantity=1,
        reference_service_life=row["RefStudyPeriod"],
        unit=Unit.kg,
        transport=None,
        results=get_assembly_results(row),
        meta_data=None,
        type="actual",
    )
    assembly = Assembly(
        id=str(uuid5(NAMESPACE_URL, row["EntityElementName"])),
        name=row["EntityElementName"],
        quantity=1.0,
        unit=Unit.kg,
        products={product.id: product},
        meta_data=None,
        type="actual",
        results=get_assembly_results(row),
    )
    project.assemblies[assembly.id] = assembly
    return project

def get_assembly_results(row: dict):
    return {
        "gwp": {
            LifeCycleStage.a1a3: float(row["A1ToA3"]),
            LifeCycleStage.a4: float(row["A4"]),
            LifeCycleStage.a5: float(row["A5"]),
            LifeCycleStage.b1: float(row["B1"]),
            LifeCycleStage.b2: float(row["B2"]),
            LifeCycleStage.b3: float(row["B3"]),
            LifeCycleStage.b4: float(row["B4"]),
            LifeCycleStage.b5: float(row["B5"]),
            LifeCycleStage.c1: float(row["C1"]),
            LifeCycleStage.c2: float(row["C2"]),
            LifeCycleStage.c3: float(row["C3"]),
            LifeCycleStage.c4: float(row["C4"]),
            LifeCycleStage.d: float(row["D"]),
        }
    }

def get_project_results(row: dict):
    return {
        "gwp": {
            LifeCycleStage.a1a3: float(row["Total_A1ToA3"]),
            LifeCycleStage.a4: float(row["Total_A4"]),
            LifeCycleStage.a5: float(row["Total_A5"]),
            LifeCycleStage.b1: float(row["Total_B1"]),
            LifeCycleStage.b2: float(row["Total_B2"]),
            LifeCycleStage.b3: float(row["Total_B3"]),
            LifeCycleStage.b4: float(row["Total_B4"]),
            LifeCycleStage.b5: float(row["Total_B5"]),
            LifeCycleStage.c1: float(row["Total_C1"]),
            LifeCycleStage.c2: float(row["Total_C2"]),
            LifeCycleStage.c3: float(row["Total_C3"]),
            LifeCycleStage.c4: float(row["Total_C4"]),
            LifeCycleStage.d: float(row["Total_D"]),
        }
    }

def add_project(row: dict):
    project = Project(
        id=row["EntityCode"].replace("BECD-", ""),
        name=row["EntityName"],
        description=row["EntityDescription"],
        assemblies={},
        reference_study_period=row["RefStudyPeriod"],
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        impact_categories=[ImpactCategoryKey.gwp],
        life_cycle_stages=[LifeCycleStage.a1a3, LifeCycleStage.a4, LifeCycleStage.a5, LifeCycleStage.b1,
                           LifeCycleStage.b2, LifeCycleStage.b3, LifeCycleStage.b4, LifeCycleStage.b5,
                           LifeCycleStage.c1, LifeCycleStage.c2, LifeCycleStage.c3, LifeCycleStage.c4,
                           LifeCycleStage.d],
        location=get_location(
            row.get("Location"), row.get("Country")
        ),
        results=get_project_results(row),
        project_info=ProjectInfo(
            type="buildingInfo",
            building_completion_year=date_to_year(row["ConstructionEndDate"]),
            building_height=ValueUnit(value=float(row["TotalHeightAboveGround"]), unit=Unit.m),
            building_footprint=ValueUnit(value=float(row["BuildingFootprint"]), unit=Unit.m),
            gross_floor_area=AreaType(
                unit=Unit.m2,
                value=float(row["SizePrimary"]) or 0,
                definition="GIA",
            ),
            building_type=get_building_type(row["ProjectType"]),
            building_typology=[BuildingTypology.unknown],
            floors_above_ground=int(row["AboveGroundStorey"]) or 0,
            floors_below_ground=row.get("UndergroundStorey", None) or None,
            general_energy_class=GeneralEnergyClass.unknown,
            roof_type=RoofType.unknown,
        ),
        meta_data={
            "source": {"name": "BECD", "url": None},
            "construction_start": row["ConstructionStartDate"],
            "construction_year_existing_building": date_to_year(row["ConstructionOriginalBuildingDate"]),
            "assessment": {
                "year": date_to_year(row["DateofAssessment"]),
                "date": row["DateofAssessment"],
                "en15978_compliance": row["AssessmentCompliantBS_EN15978"] == "Fully compliant",
                "rics_2017_compliance": row["CompliantCarbon"] == "Fully compliant with 2017 version",
                "verified": row["ThirdPartyVerification"] == "Yes",
                "verified_info": row["ThirdPartyVerificationDetail"],
                "assessor": {
                    "name": row["AssessorName"],
                    "email": row["AssessorEmail"],
                    "organization": row["AssessorAffiliation"],
                },
                "quantity_source": row["MaterialQuantitiesComeFrom"],
            },
            "cost": {
                "total_cost": float(row["ConstructionCost"]) if row["ConstructionCost"] else None,
                "currency": "gbp",
            },
            "demolished_area": {
                "value": float(row["DemolishedGIA"]) if row["DemolishedGIA"] else None,
                "unit": Unit.m2,
            },
            "newly_built_area": {
                "value": float(row["NewBuildGIA"]) if row["NewBuildGIA"] else None,
                "unit": Unit.m2,
            },
            "retrofitted_area": {
                "value": float(row["RefurbishedGIA"]) if row["RefurbishedGIA"] else None,
                "unit": Unit.m2,
            },
            "project_site_area": {
                "value": float(row["OverallSiteArea"]) if row["OverallSiteArea"] else None,
                "unit": Unit.m2,
            },
            "thermal_envelope_area": {
                "value": float(row["FacadeArea"]) if row["FacadeArea"] else 0 + float(row["RoofArea"]) if row["RoofArea"] else 0,
                "unit": Unit.m2,
            },
            "structural": {
                "column_grid_long": {
                    "value": float(row["StructuralGridX"]),
                    "unit": Unit.m,
                },
                "foundation_type": row["PSCFoundationTypePrimary"],
                "vertical_gravity_system": row["PSCVerticalElementStructureTypePrimary"],
                "secondary_vertical_gravity_system": row["PSCVerticalElementStructureTypeSecondary"],
                "horizontal_gravity_system": row["PSCHorizontalElementTypePrimary"],
                "secondary_horizontal_gravity_system": row["PSCHorizontalElementTypeSecondary"],
            }
        },
        project_phase=ProjectPhase.other,
        software_info=SoftwareInfo(
            lca_software=row["AssessmentSoftware"],
            goal_and_scope_definition=row["AssessmentScope"]
        ),
    )
    return update_project(project, row)


def save_data(data: list[dict], data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Saving BECD to file: {file}")

    file.write_text(json.dumps(data, indent=2))


def process_projects(file: Path):
    projects = {}

    with open(file) as _file:
        reader = DictReader(_file, delimiter=",")
        for index, row in enumerate(reader):
            log.debug(f"Processing row {index}")
            project_id = row["EntityCode"]
            if project_id not in projects:
                projects[project_id] = add_project(row)
            else:
                update_project(projects[project_id], row)

    return [
        project.model_dump(mode="json", by_alias=True) for project in projects.values()
    ]


def load_becd(data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Loading BECD from file: {file}")

    lcax_data = process_projects(file)

    save_data(lcax_data, data_folder, filename.replace(".csv", ".json"))


if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent / "data"
    _filename = "becd_20240926.csv"

    load_becd(_data_folder, _filename)
    log.info("BECD loaded")
