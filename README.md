<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" height="150px" srcset="docs\assets\README_logo.png">
        <img alt="logo" height="150px" src="docs/logo.png">
    </picture>
    <br>
    <h1>
    </h1>
</div>

MINDS is a framework designed to integrate multimodal oncology data. It queries and integrates data from multiple sources, including clinical data, genomic data, and imaging data from the NIH NCI CRDC and TCIA portals.

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
import minds

# get a list of all the tables in the database
tables = minds.get_tables()

# get a list of all the columns in a table
columns = minds.get_columns("clinical")

# Query the database directly
query = "SELECT * FROM nihnci.clinical WHERE project_id = 'TCGA-LUAD' LIMIT 10"
df = minds.query(query)
```

### Downloading data from MINDS

```python
# Generate a cohort to download from query
query_cohort = minds.build_cohort(query=query, output_dir="./data")

# or you can now directly supply a cohort from GDC
gdc_cohort = minds.build_cohort(gdc_cohort="cohort_Unsaved_Cohort.2024-02-12.tsv", output_dir="./data")

# To download the data from the a cohort, simply call the download method for the cohort 
# for example, downloading the gdc_cohort
gdc_cohort.download()
```

## Please cite our work

**Note**: Currently under review at the Sensors journal special issue on "Multimodal Data Fusion Technologies and Applications in Intelligent System". Till then please cite our arXiv preprint:

```bibtex
@misc{tripathi2023building,
      title={Building Flexible, Scalable, and Machine Learning-ready Multimodal Oncology Datasets}, 
      author={Aakash Tripathi and Asim Waqas and Kavya Venkatesan and Yasin Yilmaz and Ghulam Rasool},
      year={2023},
      eprint={2310.01438},
      archivePrefix={arXiv},
      primaryClass={cs.LG}
}
```

## Contributing

We welcome contributions from the community. If you would like to contribute to the MINDS project, please read our [contributing guidelines](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
