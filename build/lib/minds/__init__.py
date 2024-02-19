import os

from .aggregator import Aggregator
from .database import DatabaseManager
from .downloader import (
    GDCFileDownloader,
    TCIAFileDownloader,
)
from .update import MINDSUpdater

db = DatabaseManager()


def get_tables():
    """Get the list of tables in the database

    Returns
    -------
    list
        A list of tables in the database
    """
    return db.get_tables()


def get_columns(table):
    """Get the list of columns in a table

    Parameters
    ----------
    table : str
        The name of the table

    Returns
    -------
    list
        A list of columns in the table
    """
    return db.get_columns(table)


def query(query):
    """Query the database and return the result as a pandas dataframe

    Parameters
    ----------
    query_string : str
        The query string to be executed on the database

    Returns
    -------
    pandas.DataFrame
        The result of the query
    """
    return db.execute(query)


class Cohort:
    def __init__(self, data, output_dir):
        self.data = data
        self.output_dir = output_dir
        self.manifest_file = os.path.join(output_dir, "manifest.json")

    def generate_manifest(self):
        aggregator = Aggregator(self.data, self.output_dir)
        aggregator.generate_manifest()

    def download(self, threads=4):
        if not os.path.exists(self.manifest_file):
            raise FileNotFoundError(
                f"No manifest file found in {self.output_dir}. Please run generate_manifest first."
            )

        # Download files for the cohort
        self._download_gdc_files(threads)
        self._download_tcia_files(threads)

    def _download_gdc_files(self, threads):
        gdc_downloader = GDCFileDownloader(self.output_dir, MAX_WORKERS=threads)
        gdc_downloader.process_cases()

    def _download_tcia_files(self, threads):
        tcia_downloader = TCIAFileDownloader(self.output_dir, MAX_WORKERS=threads)
        tcia_downloader.process_cases()


def build_cohort(output_dir, query=None, gdc_cohort=None):
    """Builds a cohort based on a query or a GDC cohort file and returns a Cohort object."""
    if query:
        cohort_data = db.get_minds_cohort(query)
    elif gdc_cohort:
        cohort_data = db.get_gdc_cohort(gdc_cohort)
    else:
        raise ValueError("Either a query or a gdc_cohort file must be provided")

    cohort = Cohort(cohort_data, output_dir)
    cohort.generate_manifest()
    return cohort


def update(skip_download=False):
    """Update the database with the latest data from data sources

    Returns
    -------
    None
    """
    updater = MINDSUpdater()
    if not skip_download:
        updater.threaded_update()
    db.update(updater.temp_folder)
