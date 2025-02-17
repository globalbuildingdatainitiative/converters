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

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def load_slice(_data_folder: Path, _filename: str) -> None:
    log.info(f"Loading SLiCE: {_filename}")

    data = json.loads((_data_folder / _filename).read_text())
    log.info(f"Number of elements: {len(data)}")

    index = 0
    for chunk in chunks(data, 50):
        log.info(f"Processing chunk {index}")
        path = _data_folder / _filename.replace(".json", f"_{index}.json")
        path.write_text(json.dumps(chunk, indent=2))
        index += 1

if __name__ == "__main__":
    _data_folder = Path(__file__).parent.parent.parent / "data"
    file_names = ["slice_belgium_20250212.json", "slice_austria_20250212.json", "slice_20240319.json"]

    for _filename in file_names:
        load_slice(_data_folder, _filename)

    log.info("SLiCE loaded")