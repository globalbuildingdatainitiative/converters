# Global Building Data Initiative (GBDI)

The [Global Building Data Initiative (GBDI)](www.gbdi.io) addresses the urgent need for open building data to inform action for reducing resource use as well as embodied and whole life carbon (WLC) emissions within the built environment. GBDI aims to fill prevalent gaps by encouraging collaboration with diverse stakeholders and advancing the adoption of sustainable building practices worldwide, with the development of a global network of stakeholders, the Open Building Data Format (openBDF) and the [Open Building Data Platform](https://www.gbdi.io/platform) for building data collection and management, enabling visualization, benchmarking, and target-setting to support the development of specific pathways for WLC reduction. Learn more about GBDI via [www.gbdi.io](www.gbdi.io).

# GBDI Converters

This repository contains parsers for converting various data sources for the [GBDI](https://app.gbdi.io) app to ingest.

## Description

The GBDI app collects building component and whole building LCA (Life Cycle Assessment) data on a global scale to
provide benchmarks and analytics of whole building LCA across the globe.

## Formats

Current formats supported:

- [Structural Panda](src/structural_panda/structural_panda.py)
- [CarbEnMats](src/carbenmats/carbenmats.py)
- [SLiCE](src/slice/slice.py)
- [BECD](src/becd/becd.py)

## Installation

This repository uses [UV](https://docs.astral.sh/uv/) for dependency management. \
To install the dependencies, run:

```sh
uv install
```

## Usage

To run a converter, simply run the python file in the src directory. For example, to run the structural panda converter,
run the following command:

```
python3 src/structural_panda/structural_panda.py
```

# Converted Data

Input files as well as converted files are provided in the `data/` directory.
You can use the input files to test the converters or simply use the already converted files.

# License

This project is licensed under the terms of Apache v2.0, described in the [LICENSE](LICENSE) file.
