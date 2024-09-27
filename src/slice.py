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
    yaml.safe_load((Path(__file__).parent / "logging.yaml").read_text())
)
log = logging.getLogger(__name__)


def get_location(region: str):
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


def get_building_typology(data: str):
    if data.lower() in [
        "single-family house",
        "multi-family house",
        # "semi-detached",
        # "row house",
    ]:
        return [BuildingTypology.residential]
    elif data.lower() in ["office"]:
        return [BuildingTypology.office]
    # elif data.lower() in [
    #     "school and daycare",
    #     "hospital and health",
    #     "art & culture",
    #     "sport & entertainment",
    # ]:
    #     return [BuildingTypology.public]
    # elif data.lower() in ["hotel & resort", "retail and restaurant"]:
    #     return [BuildingTypology.commercial]
    # elif data.lower() in ["aviation"]:
    #     return [BuildingTypology.infrastructure]
    # elif data.lower() in ["technology & science"]:
    #     return [BuildingTypology.industrial]
    # elif data.lower() in ["other", "mixed use", "no data"]:
    #     return [BuildingTypology.other]
    else:
        raise NotImplementedError(f"Unknown building typology: {data}")


def get_building_type(data: str):
    if data.lower() in ["new buildings"]:
        return BuildingType.new_construction_works
    # elif data.lower() in ["no data"]:
    #     return BuildingType.other
    elif data.lower() in ["refurbishment"]:
        return BuildingType.retrofit_works
    elif data.lower() in ["existing buildings"]:
        return BuildingType.operations
    else:
        raise NotImplementedError(f"Unknown building type: {data}")


def get_general_energy_class(data: str):
    if data.lower() in ["standard"]:
        return GeneralEnergyClass.standard
    elif data.lower() in ["advanced"]:
        return GeneralEnergyClass.advanced
    elif data.lower() in ["average"]:
        return GeneralEnergyClass.existing
    # elif data.lower() in ["no data"]:
    #     return GeneralEnergyClass.unknown
    else:
        raise NotImplementedError(f"Unknown energy class: {data}")


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
        stage = LCAxLifeCycleStage.from_str(row["LCS_EN15978"]).value
        impact_sets = [
            (ImpactCategoryKey.gwp, "ind_GWP_Tot"),
            (ImpactCategoryKey.gwp_bio, "ind_GWP_Bio"),
            (ImpactCategoryKey.gwp_lul, "ind_GWP_LuLuc"),
            (ImpactCategoryKey.odp, "ind_ODP"),
            (ImpactCategoryKey.ap, "ind_AP"),
            (ImpactCategoryKey.ep_fw, "ind_EP_Fw"),
            (ImpactCategoryKey.ep_mar, "ind_EP_Mar"),
            (ImpactCategoryKey.ep_ter, "ind_EP_Ter"),
            (ImpactCategoryKey.pocp, "ind_PCOP"),
            (ImpactCategoryKey.wdp, "ind_WDP"),
            (ImpactCategoryKey.pm, "ind_PM"),
            (ImpactCategoryKey.irp, "ind_IRP"),
            (ImpactCategoryKey.etp_fw, "ind_ETP_Fw"),
            (ImpactCategoryKey.htp_c, "ind_HTP_c"),
            (ImpactCategoryKey.htp_nc, "ind_HTP_nc"),
            (ImpactCategoryKey.sqp, "ind_SQP"),
        ]

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
            id=str(uuid5(NAMESPACE_URL, row["techflow_name_mmg"])),
            name=row["techflow_name_mmg"],
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
                    row["worksection_class_sfb"] or "" + row["techflow_name_mmg"],
                )
            ),
            name=row["worksection_class_sfb"] or row["techflow_name_mmg"],
            description="",
            reference_service_life=50,
            impact_data=LCAxTechFlow.from_row(row),
            # quantity=row["amount_material_kg_per_building"] or 0,
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
            id=str(uuid5(NAMESPACE_URL, row["element_class_sfb"])),
            name=row["element_class_generic_name"],
            classification=[
                Classification(
                    system="SfB",
                    code=row["element_class_sfb"],
                    name=row["element_class_generic_name"],
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
            id=str(uuid5(NAMESPACE_URL, row["building_archetype_code"])),
            name="Unknown",
            location=get_location(row["stock_region_name"]),
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
                building_type=get_building_type(row["stock_activity_type_name"]),
                building_typology=get_building_typology(
                    row["building_use_subtype_name"]
                ),
                floors_above_ground=1,
                general_energy_class=get_general_energy_class(
                    row["building_energy_performance_name"]
                ),
                roof_type=RoofType.unknown,
            ),
        )


def update_project(project: LCAxProject, row: dict):
    assembly_id = str(uuid5(NAMESPACE_URL, row["element_class_sfb"]))
    product_id = str(
        uuid5(
            NAMESPACE_URL, row["worksection_class_sfb"] or "" + row["techflow_name_mmg"]
        )
    )

    if assembly_id not in project.assemblies:
        project.assemblies[assembly_id] = LCAxAssembly.from_row(row)

    if product_id not in project.assemblies[assembly_id].products:
        project.assemblies[assembly_id].products[product_id] = LCAxProduct.from_row(row)
    else:
        project.assemblies[assembly_id].products[product_id].add_row(row)


def get_projects_by_archetypes(file: Path, archetypes: list[str]):
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


def get_archetypes():
    return [
        "CON_MFH_EXB_AVG",
        "CON_MFH_NEW_ADV",
        "CON_MFH_NEW_STD",
        "CON_MFH_REF_ADV",
        "CON_MFH_REF_STD",
        "CON_OFF_EXB_AVG",
        "CON_OFF_NEW_ADV",
        "CON_OFF_NEW_STD",
        "CON_OFF_REF_ADV",
        "CON_OFF_REF_STD",
        "CON_SFH_EXB_AVG",
        "CON_SFH_NEW_ADV",
        "CON_SFH_NEW_STD",
        "CON_SFH_REF_ADV",
        "CON_SFH_REF_STD",
        # ---
        "MED_MFH_EXB_AVG",
        "MED_MFH_NEW_ADV",
        "MED_MFH_NEW_STD",
        "MED_MFH_REF_ADV",
        "MED_MFH_REF_STD",
        "MED_OFF_EXB_AVG",
        "MED_OFF_NEW_ADV",
        "MED_OFF_NEW_STD",
        "MED_OFF_REF_ADV",
        "MED_OFF_REF_STD",
        "MED_SFH_EXB_AVG",
        "MED_SFH_NEW_ADV",
        "MED_SFH_NEW_STD",
        "MED_SFH_REF_ADV",
        "MED_SFH_REF_STD",
        # ---
        "NOR_MFH_EXB_AVG",
        "NOR_MFH_NEW_ADV",
        "NOR_MFH_NEW_STD",
        "NOR_MFH_REF_ADV",
        "NOR_MFH_REF_STD",
        "NOR_OFF_EXB_AVG",
        "NOR_OFF_NEW_ADV",
        "NOR_OFF_NEW_STD",
        "NOR_OFF_REF_ADV",
        "NOR_OFF_REF_STD",
        "NOR_SFH_EXB_AVG",
        "NOR_SFH_NEW_ADV",
        "NOR_SFH_NEW_STD",
        "NOR_SFH_REF_ADV",
        "NOR_SFH_REF_STD",
        # ---
        "OCE_MFH_EXB_AVG",
        "OCE_MFH_NEW_ADV",
        "OCE_MFH_NEW_STD",
        "OCE_MFH_REF_ADV",
        "OCE_MFH_REF_STD",
        "OCE_OFF_EXB_AVG",
        "OCE_OFF_NEW_ADV",
        "OCE_OFF_NEW_STD",
        "OCE_OFF_REF_ADV",
        "OCE_OFF_REF_STD",
        "OCE_SFH_EXB_AVG",
        "OCE_SFH_NEW_ADV",
        "OCE_SFH_NEW_STD",
        "OCE_SFH_REF_ADV",
        "OCE_SFH_REF_STD",
    ]


def load_slice(data_folder: Path, filename: str):
    file = data_folder / filename
    log.info(f"Loading SLiCE from file: {file}")

    archetypes = get_archetypes()
    lcax_data = get_projects_by_archetypes(file, archetypes)

    save_data(lcax_data, data_folder, filename.replace(".parquet", ".json"))


if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent / "data"
    _filename = "slice_20240319.parquet"

    load_slice(_data_folder, _filename)
    log.info("SLiCE loaded")
