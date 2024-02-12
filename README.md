<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" height="200px" srcset="docs/logo.png">
        <img alt="logo" height="200px" src="docs/logo.png" style="border-radius: 25px;">
    </picture>
    <br>
    <h1>
        Multimodal Integration of Oncology Data System (MINDS)
    </h1>
</div>

MINDS is a system designed to integrate multimodal oncology data. It queries and integrates data from multiple sources, including clinical data, genomic data, and imaging data from the NIH NCI CRDC and TCIA portals.

## Installation

Currently the cloud version of MINDS is in closed beta, but, you can still recreate the MINDS database locally. To get the local version of the MINDS database running, you will need to setup a MySQL database and populate it with the MINDS schema. This can be easily done using a docker container. First, you will need to install docker. You can find the installation instructions for your operating system [here](https://docs.docker.com/get-docker/). Next, you will need to pull the MySQL docker image and run a container with the following command. Please replace `my-secret-pw` with your desired password and `port` with the port you want to use to access the database. The default port for MySQL is 3306. The following command will create a MySQL container with the name `minds` and the database `minds` with the password `my-secret-pw` and the port `port` exposed to the host machine.

```bash
docker run -d --name minds -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=minds -p port:3306 mysql
```

Finally, to install the MINDS python package use the following pip command:

```bash
pip install git+https://github.com/lab-rasool/MINDS.git
```

After installing the package, please create a .env file in the root directory of the project with the following variables:

```bash
HOST=       # default host name: localhost
PORT=       # default port: 3306
DB_USER=    # the default user: root
PASSWORD=   # the password you set for the MySQL container
DATABASE=minds   
```

## Usage

### Initial setup and automated updates

If you have locally setup the MINDS database, then you will need to populate it with data. To do this, or to update the database with the latest data, you can use the following command:

```python
# Import the minds package
import minds

# Update the database with the latest data
minds.update()
```

### Querying the MINDS database

The MINDS python package provides a python interface to the MINDS database. You can use this interface to query the database and return the results as a pandas dataframe.

```python
# get a list of all the tables in the database
tables = minds.get_tables()

# get a list of all the columns in a table
columns = minds.get_columns("clinical")

# Query the database directly
query = "SELECT * FROM minds.clinical WHERE project_id = 'TCGA-LUAD'"
df = minds.query(query)
```

### Downloading data from MINDS

```python
# Generate a cohort to download
cohort = minds.get_cohort(query)
# Set the output directory
save_loc = "/data/TCGA-LUAD"

# Download the data 
minds.download(cohort=cohort, output_dir=save_loc)
```
