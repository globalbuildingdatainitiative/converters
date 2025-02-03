import importlib.metadata
import json
import logging.config
from enum import Enum
from pathlib import Path
from typing import Self, Type
from uuid import NAMESPACE_URL, uuid5

import pyarrow.parquet as pq
import yaml
from lcax import (
    Project,
    LifeCycleStage,
    Location,
    Country,
    SoftwareInfo,
    ImpactCategoryKey,
    Unit,
    Assembly,
    Product,
    ProjectPhase,
    ProjectInfo1 as ProjectInfo,
    AreaType,
    RoofType,
    BuildingType,
    BuildingTypology,
    GeneralEnergyClass,
    Classification,
    ReferenceSourceForImpactDataSource2 as TechFlow,
    calculate_project,
)

logging.config.dictConfig(
    yaml.safe_load((Path(__file__).parent.parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)

MAPPING = json.loads((Path(__file__).parent / "mapping.json").read_text())

def get_location(row: dict):
    region = row[MAPPING["location.country"]]
    region = region.lower()
    if region == "continental":
        country = Country.deu
    elif region == "mediterranean":
        country = Country.ita
    elif region == "nordic":
        country = Country.swe
    elif region == "oceanic":
        country = Country.gbr
    else:
        country = Country.deu

    return Location(country=country, city=None, address=None)


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


class LCAxLifeCycleStage(Enum):
    a0 = "a0"
    a1a3 = "a1a3"
    a4 = "a4"
    a5 = "a5"
    b1 = "b1"
    b2 = "b2"
    b3 = "b3"
    b4 = "b4"
    b5 = "b5"
    b6 = "b6"
    b7 = "b7"
    b8 = "b8"
    c1 = "c1"
    c2 = "c2"
    c3 = "c3"
    c4 = "c4"
    d = "d"

    @staticmethod
    def from_str(name: str):
        _name = name.lower()
        if _name == "a0":
            return LCAxLifeCycleStage.a0
        elif _name == "a1-3":
            return LCAxLifeCycleStage.a1a3
        elif _name == "a4":
            return LCAxLifeCycleStage.a4
        elif _name == "a5":
            return LCAxLifeCycleStage.a5
        elif _name == "b1":
            return LCAxLifeCycleStage.b1
        elif _name == "b2":
            return LCAxLifeCycleStage.b2
        elif _name == "b3":
            return LCAxLifeCycleStage.b3
        elif _name == "b4":
            return LCAxLifeCycleStage.b4
        elif _name == "b5":
            return LCAxLifeCycleStage.b5
        elif _name == "b6":
            return LCAxLifeCycleStage.b6
        elif _name == "b7":
            return LCAxLifeCycleStage.b7
        elif _name == "b8":
            return LCAxLifeCycleStage.b8
        elif _name == "c1":
            return LCAxLifeCycleStage.c1
        elif _name == "c2":
            return LCAxLifeCycleStage.c2
        elif _name == "c3":
            return LCAxLifeCycleStage.c3
        elif _name == "c4":
            return LCAxLifeCycleStage.c4
        elif _name == "d":
            return LCAxLifeCycleStage.d
        else:
            raise NotImplementedError(f"Unknown life cycle stage: {_name}")


class LCAxTechFlow(TechFlow):
    def add_impact(self, key: ImpactCategoryKey, stage: str, value: float):
        if stage not in self.impacts[key.value]:
            self.impacts[key.value][stage] = 0
        self.impacts[key.value][stage] += value

    def add_row(self, row: dict):
        stage = LCAxLifeCycleStage.from_str(row[MAPPING["life_cycle_stage"]]).value
        impact_sets = [(ImpactCategoryKey[key.split(".")[1]], MAPPING[key]) for key in MAPPING.keys() if key.startswith("impact_category_key.")]

        for impact in impact_sets:
            self.add_impact(impact[0], stage, row[impact[1]])

    @classmethod
    def from_row(cls: Type[Self], row: dict) -> Self:
        impacts = {
            ImpactCategoryKey.gwp: {},
            ImpactCategoryKey.gwp_bio: {},
            ImpactCategoryKey.gwp_lul: {},
            ImpactCategoryKey.odp: {},
            ImpactCategoryKey.ap: {},
            ImpactCategoryKey.ep_fw: {},
            ImpactCategoryKey.ep_mar: {},
            ImpactCategoryKey.ep_ter: {},
            ImpactCategoryKey.pocp: {},
            ImpactCategoryKey.wdp: {},
            ImpactCategoryKey.pm: {},
            ImpactCategoryKey.irp: {},
            ImpactCategoryKey.etp_fw: {},
            ImpactCategoryKey.htp_c: {},
            ImpactCategoryKey.htp_nc: {},
            ImpactCategoryKey.sqp: {},
        }
        tech_flow = cls(
            id=str(uuid5(NAMESPACE_URL, row[MAPPING["assemblies.products.impact_data.id"]])),
            name=row[MAPPING["assemblies.products.impact_data.name"]],
            declared_unit=Unit.kg,
            format_version=importlib.metadata.version("lcax"),
            source=None,
            comment=None,
            location=Country.unknown,
            conversions=None,
            impacts=impacts,
            meta_data=None,
            type="actual",
        )
        tech_flow.add_row(row)
        return tech_flow


class LCAxProduct(Product):
    def add_row(self, row: dict):
        self.impact_data.add_row(row)

    @classmethod
    def from_row(cls: Type[Self], row: dict) -> Self:
        return cls(
            id=str(
                uuid5(
                    NAMESPACE_URL,
                    row[MAPPING["assemblies.products.id"][0]] or "" + row[MAPPING["assemblies.products.id"][1]]
                )
            ),
            name=row[MAPPING["assemblies.products.name"][0]] or "" + row[MAPPING["assemblies.products.name"][1]],
            description="",
            reference_service_life=50,
            impact_data=LCAxTechFlow.from_row(row),
            quantity=1,
            unit=Unit.kg,
            transport=None,
            results=None,
            meta_data=None,
            type="actual",
        )


class LCAxAssembly(Assembly):
    @classmethod
    def from_row(cls: Type[Self], row: dict) -> Self:
        return cls(
            id=str(uuid5(NAMESPACE_URL, row[MAPPING["assemblies.id"]])),
            name=row[MAPPING["assemblies.name"]],
            classification=[
                Classification(
                    system="SfB",
                    code=row[MAPPING["assemblies.classification.code"]],
                    name=row[MAPPING["assemblies.classification.name"]],
                )
            ],
            quantity=1.0,
            unit=Unit.kg,
            products={},
            results=None,
            meta_data=None,
            type="actual",
        )


class LCAxProject(Project):
    @classmethod
    def from_row(cls, row: dict):
        return cls(
            id=str(uuid5(NAMESPACE_URL, row[MAPPING["id"]])),
            name="Undefined",
            location=get_location(row),
            impact_categories=[
                ImpactCategoryKey.gwp,
                ImpactCategoryKey.gwp_bio,
                ImpactCategoryKey.gwp_lul,
                ImpactCategoryKey.odp,
                ImpactCategoryKey.ap,
                ImpactCategoryKey.ep_fw,
                ImpactCategoryKey.ep_mar,
                ImpactCategoryKey.ep_ter,
                ImpactCategoryKey.pocp,
                ImpactCategoryKey.wdp,
                ImpactCategoryKey.pm,
                ImpactCategoryKey.irp,
                ImpactCategoryKey.etp_fw,
                ImpactCategoryKey.htp_c,
                ImpactCategoryKey.htp_nc,
                ImpactCategoryKey.sqp,
            ],
            life_cycle_stages=[
                LifeCycleStage.a1a3,
                LifeCycleStage.a4,
                LifeCycleStage.a5,
                LifeCycleStage.b2,
                LifeCycleStage.b4,
                LifeCycleStage.b5,
                LifeCycleStage.b6,
                LifeCycleStage.c1,
                LifeCycleStage.c2,
                LifeCycleStage.c3,
                LifeCycleStage.c4,
            ],
            owner=None,
            format_version=importlib.metadata.version("lcax"),
            classification_system="SfB",
            software_info=SoftwareInfo(lca_software="SLiCE"),
            assemblies={},
            project_phase=ProjectPhase.other,
            project_info=ProjectInfo(
                type="buildingInfo",
                gross_floor_area=AreaType(
                    unit=Unit.m2,
                    value=1,
                    definition="",
                ),
                building_type=get_building_type(row),
                building_typology=get_building_typology(row),
                floors_above_ground=1,
                general_energy_class=get_general_energy_class(row),
                roof_type=RoofType.unknown,
            ),
            meta_data={"source": { "name": "SLiCE", "url": None}},
        )


def update_project(project: LCAxProject, row: dict):
    assembly_id = str(uuid5(NAMESPACE_URL, row[MAPPING["assemblies.id"]]))
    product_id = str(
        uuid5(
            NAMESPACE_URL, row[MAPPING["assemblies.products.id"][0]] or "" + row[MAPPING["assemblies.products.id"][1]]
        )
    )

    if assembly_id not in project.assemblies:
        project.assemblies[assembly_id] = LCAxAssembly.from_row(row)

    if product_id not in project.assemblies[assembly_id].products:
        project.assemblies[assembly_id].products[product_id] = LCAxProduct.from_row(row)
    else:
        project.assemblies[assembly_id].products[product_id].add_row(row)


def get_projects_by_archetypes(file: Path):
    wanted_fields = [
        "stock_region_name",
        "building_use_subtype_name",
        "stock_activity_type_name",
        "building_energy_performance_name",
        "building_archetype_code",
        "element_class_generic_name",
        "element_class_sfb",
        "worksection_class_sfb",
        "techflow_name_mmg",
        "material_name_mmg",
        "material_name_JRC_CDW",
        "material_category_Sturm",
        "material_category_Sturm_upd",
        "amount_material_kg_per_building",
        "activity_year",
        "LCS_EN15978",
        "ind_GWP_Tot",
        "ind_GWP_Fos",
        "ind_GWP_Bio",
        "ind_GWP_LuLuc",
        "ind_ODP",
        "ind_AP",
        "ind_EP_Fw",
        "ind_EP_Mar",
        "ind_EP_Ter",
        "ind_PCOP",
        "ind_ADP_MiMe",
        "ind_ADP_Fos",
        "ind_WDP",
        "ind_PM",
        "ind_IRP",
        "ind_ETP_Fw",
        "ind_HTP_c",
        "ind_HTP_nc",
        "ind_SQP",
    ]

    # log.debug(f"Meta data: {pq.read_table(file).schema.metadata}")
    table = pq.read_table(file, columns=wanted_fields).to_pylist()

    projects = {}

    for row in table:
        archetype = row["building_archetype_code"]
        if archetype not in projects:
            log.debug(f"Adding archetype: {archetype}")
            projects[archetype] = LCAxProject.from_row(row)

        update_project(projects[archetype], row)

    for key, project in projects.items():
        log.debug(f"Calculating results for: {key}")
        projects[key] = calculate_project(project)

    return [
        project.model_dump(mode="json", by_alias=True) for project in projects.values()
    ]


def save_data(data: list[dict], data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Saving SLiCE to file: {file}")

    file.write_text(json.dumps(data, indent=2))


def load_slice(data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Loading SLiCE from file: {file}")

    lcax_data = get_projects_by_archetypes(file)

    save_data(lcax_data, data_folder, filename.replace(".parquet", ".json"))


if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent.parent / "data"
    _filename = "slice_20240319.parquet"

    load_slice(_data_folder, _filename)
    log.info("SLiCE loaded")
