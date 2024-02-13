
# Welcome to the MINDS Database Documentation

<!-- <p align="left">
    <img src="logo.png" alt="Logo" width="200"/>
</p>
Multimodal Integration of Oncology Data System -->

<html>
<head>
    <style>
        .container {
            display: flex;
            align-items: center; /* Aligns items vertically in the center */
        }
        .text {
            margin-left: 20px; /* Adjusts spacing between the logo and the text */
            font-size: 40px; /* Adjust the font size as needed */
            font-weight: bold; /* Makes the text bold */
            /* Add more styling as needed */
        }
    </style>
</head>
<body>
<div class="container">
    <p align="left">
        <img src="assets\logo.png" alt="Logo" width="250"/>
    </p>
    <div class="text">
        <span style="display:block;font-weight:bold;font-size:40px;"><u>M</u>ultimodal <u>I</u>ntegration of O<u>n</u>cology <u>D</u>ata <u>S</u>ystem</span>
    </div>
</div>
</body>
</html>



MINDS is a system designed to integrate multimodal oncology data. It queries and integrates data from multiple sources, including clinical data, genomic data, and imaging data from the NIH NCI CRDC and TCIA portals.

## Installation

Currently the cloud version of MINDS is in closed beta, but, you can still recreate the MINDS database locally. To get the local version of the MINDS database running, you will need to setup a MySQL database and populate it with the MINDS schema. This can be easily done using a docker container. First, you will need to install docker. You can find the installation instructions for your operating system [here](https://docs.docker.com/get-docker/). Next, you will need to pull the MySQL docker image and run a container with the following command.

**NOTE:** Please replace `my-secret-pw` with your desired password and `port` with the port you want to use to access the database. The default port for MySQL is 3306. The following command will not work until you replace `port` with a valid port number.

```bash
docker run -d --name minds -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=minds -p port:3306 mysql
```

Finally, to install the MINDS python package use the following pip command:

```bash
pip install git+https://github.com/lab-rasool/MINDS.git
```

After installing the package, please create a .env file in the root directory of the project with the following variables:

```bash
HOST=localhost
PORT=3306
DB_USER=root
PASSWORD=my-secret-pw
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
