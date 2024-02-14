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


def build_cohort(output_dir, query=None, gdc_cohort=None):
    if query:
        cohort = db.get_minds_cohort(query)
    elif gdc_cohort:
        cohort = db.get_gdc_cohort(gdc_cohort)
    else:
        raise ValueError("Either a query or a .tsv gdc_cohort file must be provided")
    aggregator = Aggregator(cohort, output_dir)
    aggregator.generate_manifest()
    cohort.download = lambda: download(output_dir=output_dir)
    return cohort


def download(output_dir, threads=4):

    if not os.path.exists(f"{output_dir}/manifest.json"):
        raise ValueError(
            f"No manifest file found in {output_dir}, please run minds.build_cohort first. \n \
            If you have already run minds.build_cohort, \n \
            then please make sure that the output_dir is correct."
        )

    MAX_WORKERS = threads
    gdc_download = GDCFileDownloader(
        output_dir,
        MAX_WORKERS=MAX_WORKERS,
    )
    gdc_download.process_cases()

    # tcia_download = TCIAFileDownloader(
    #     output_dir,
    #     MAX_WORKERS=MAX_WORKERS,
    # )
    # tcia_download.process_cases()


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
