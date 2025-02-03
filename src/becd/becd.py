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
    GeneralEnergyClass,
    calculate_project,
)

logging.config.dictConfig(
    yaml.safe_load((Path(__file__).parent.parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)

MAPPING = json.loads((Path(__file__).parent / "mapping.json").read_text())


def date_to_year(date: str):
    if date:
        return datetime.strptime(date, "%d/%m/%Y %H:%M:%S").year
    else:
        return None


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
        id=str(uuid5(NAMESPACE_URL, row[MAPPING["assemblies.products.id"]])),
        name=row[MAPPING["assemblies.products.name"]],
        description="",
        impact_data=TechFlow(
            id=str(
                uuid5(NAMESPACE_URL, row[MAPPING["assemblies.products.impact_data.id"]])
            ),
            name=row[MAPPING["assemblies.products.impact_data.id"]],
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
        reference_service_life=row[
            MAPPING["assemblies.products.reference_service_life"]
        ],
        unit=Unit.kg,
        transport=None,
        results=get_assembly_results(row),
        meta_data=None,
        type="actual",
    )
    assembly = Assembly(
        id=str(uuid5(NAMESPACE_URL, row[MAPPING["assemblies.id"]])),
        name=row[MAPPING["assemblies.name"]],
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
    results = {ImpactCategoryKey.gwp: {}}
    result_items = [
        (key, value)
        for key, value in MAPPING.items()
        if key.startswith("assemblies.results.") and value
    ]
    for key, value in result_items:
        results[ImpactCategoryKey[key.split(".")[2]]][
            LifeCycleStage[key.split(".")[3]]
        ] = float(row[value])
    return results


def get_project_results(row: dict):
    results = {ImpactCategoryKey.gwp: {}}
    result_items = [
        (key, value)
        for key, value in MAPPING.items()
        if key.startswith("results.") and value
    ]
    for key, value in result_items:
        results[ImpactCategoryKey[key.split(".")[1]]][
            LifeCycleStage[key.split(".")[2]]
        ] = float(row[value])
    return results


def get_thermal_envelope_area(row: dict):
    values = [
        float(row[key] or 0) for key in MAPPING["meta_data.thermal_envelope_area.value"]
    ]
    return {
        "value": sum(values),
        "unit": Unit.m2,
    }


def add_project(row: dict):
    results = get_project_results(row)

    project = Project(
        id=row[MAPPING["id"]].replace("BECD-", ""),
        name=row[MAPPING["name"]],
        description=row[MAPPING["description"]],
        assemblies={},
        reference_study_period=row[MAPPING["reference_study_period"]],
        classification_system=None,
        format_version=importlib.metadata.version("lcax"),
        impact_categories=list(results.keys()) if results else [],
        life_cycle_stages=list(results[ImpactCategoryKey.gwp].keys())
        if results
        else [],
        location=get_location(
            row[MAPPING["location.city"]], row[MAPPING["location.country"]]
        ),
        results=results,
        project_info=ProjectInfo(
            type="buildingInfo",
            building_completion_year=date_to_year(
                row[MAPPING["project_info.building_completion_year"]]
            ),
            building_height=ValueUnit(
                value=float(row[MAPPING["project_info.building_height.value"]]),
                unit=Unit.m,
            ),
            building_footprint=ValueUnit(
                value=float(row[MAPPING["project_info.building_footprint.value"]]),
                unit=Unit.m,
            ),
            gross_floor_area=AreaType(
                unit=Unit.m2,
                value=float(row[MAPPING["project_info.gross_floor_area.value"]]) or 0,
                definition="GIA",
            ),
            building_type=get_building_type(row),
            building_typology=[BuildingTypology.unknown],
            floors_above_ground=int(row[MAPPING["project_info.floors_above_ground"]])
            or 0,
            floors_below_ground=row[MAPPING["project_info.floors_below_ground"]]
            or None,
            general_energy_class=GeneralEnergyClass.unknown,
            roof_type=RoofType.unknown,
        ),
        meta_data={
            "source": {"name": "BECD", "url": None},
            "construction_start": row[MAPPING["meta_data.construction_start"]],
            "construction_year_existing_building": date_to_year(
                row[MAPPING["meta_data.construction_year_existing_building"]]
            ),
            "assessment": {
                "year": date_to_year(row[MAPPING["meta_data.assessment.year"]]),
                "date": row[MAPPING["meta_data.assessment.date"]],
                "en15978_compliance": row[
                    MAPPING["meta_data.assessment.en15978_compliance"]
                ]
                == "Fully compliant",
                "rics_2017_compliance": row[
                    MAPPING["meta_data.assessment.rics_2017_compliance"]
                ]
                == "Fully compliant with 2017 version",
                "verified": row[MAPPING["meta_data.assessment.verified"]] == "Yes",
                "verified_info": row[MAPPING["meta_data.assessment.verified_info"]],
                "assessor": {
                    "name": row[MAPPING["meta_data.assessment.assessor.name"]],
                    "email": row[MAPPING["meta_data.assessment.assessor.email"]],
                    "organization": row[
                        MAPPING["meta_data.assessment.assessor.organization"]
                    ],
                },
                "quantity_source": row[MAPPING["meta_data.assessment.quantity_source"]],
            },
            "cost": {
                "total_cost": float(row[MAPPING["meta_data.cost.total_cost"]]),
                "currency": "gbp",
            }
            if row[MAPPING["meta_data.cost.total_cost"]]
            else None,
            "demolished_area": {
                "value": float(row[MAPPING["meta_data.demolished_area.value"]]),
                "unit": Unit.m2,
            }
            if row[MAPPING["meta_data.demolished_area.value"]]
            else None,
            "newly_built_area": {
                "value": float(row[MAPPING["meta_data.newly_built_area.value"]]),
                "unit": Unit.m2,
            }
            if row[MAPPING["meta_data.newly_built_area.value"]]
            else None,
            "retrofitted_area": {
                "value": float(row[MAPPING["meta_data.retrofitted_area.value"]]),
                "unit": Unit.m2,
            }
            if row[MAPPING["meta_data.retrofitted_area.value"]]
            else None,
            "project_site_area": {
                "value": float(row[MAPPING["meta_data.project_site_area.value"]]),
                "unit": Unit.m2,
            }
            if row[MAPPING["meta_data.project_site_area.value"]]
            else None,
            "thermal_envelope_area": get_thermal_envelope_area(row),
            "structural": {
                "column_grid_long": {
                    "value": float(
                        row[MAPPING["meta_data.structural.column_grid_long.value"]]
                    ),
                    "unit": Unit.m,
                },
                "foundation_type": row[MAPPING["meta_data.structural.foundation_type"]],
                "vertical_gravity_system": row[
                    MAPPING["meta_data.structural.vertical_gravity_system"]
                ],
                "secondary_vertical_gravity_system": row[
                    MAPPING["meta_data.structural.secondary_vertical_gravity_system"]
                ],
                "horizontal_gravity_system": row[
                    MAPPING["meta_data.structural.horizontal_gravity_system"]
                ],
                "secondary_horizontal_gravity_system": row[
                    MAPPING["meta_data.structural.secondary_horizontal_gravity_system"]
                ],
            },
        },
        project_phase=ProjectPhase.other,
        software_info=SoftwareInfo(
            lca_software=row[MAPPING["software_info.lca_software"]],
            goal_and_scope_definition=row[
                MAPPING["software_info.goal_and_scope_definition"]
            ],
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
            project_id = row[MAPPING["id"]]
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
    _data_folder = Path(__file__).parent.parent.parent / "data"
    _filename = "becd_20240926.csv"

    load_becd(_data_folder, _filename)
    log.info("BECD loaded")
