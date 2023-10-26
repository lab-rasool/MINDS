# Multimodal Integration of Oncology Data System (MINDS)

MINDS is a system designed to integrate multimodal oncology data. It queries and integrates data from multiple sources, including clinical data, genomic data, and imaging data from the NIH NCI CRDC's portals.

## Installation

Currently the cloud version of MINDS is in closed beta. But you can still recreate the MINDS database locally using the instructions in  [docs/local_install.md](docs/local_install.md).

If you have access to the cloud version of MINDS, you can install the MINDS python package using pip:

```bash
pip install git+https://github.com/lab-rasool/MINDS.git
```

After installing the package, please create a .env file in the root directory of the project with the following variables:

```bash
HOST=  # the aws host name
PORT=  # default: 3306
DB_USER= # the aws database user
PASSWORD= # the aws database password
DATABASE= # default: nihnci
```

## Usage

### Querying the MINDS database

The MINDS python package provides a python interface to the MINDS database. You can use this interface to query the database and return the results as a pandas dataframe.

```python
import MINDS as MINDS

# Create a MINDS object
minds = MINDS.MINDS()

# Query the database
query = "SELECT case_id FROM clinical WHERE project_id = 'CPTAC-2'"
case_ids = minds.get_cohort(query)
```

### Downloading data from the GDC

```python
from MINDS.downloader import GDCFileDownloader

# Download the data from the GDC for the give cases in the query
download_dir = # provide a path to a directory to download the data to, ex: r"F:\\Projects\\FMT\\data"
gdc_downloader = GDCFileDownloader(DATA_DIR=download_dir)
gdc_downloader.process_cases(case_ids)
```
