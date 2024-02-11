from .downloader import GDCFileDownloader, IDCFileDownloader, TCIAFileDownloader
from .database import DatabaseManager
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


def get_cohort(query):
    """Query the database and return the case ids and case_submitter_ids as two lists

    Parameters
    ----------
    query_string : str
        The query string to be executed on the database

    Returns
    -------
    dataframe
        A df containing unique case_ids and their case_submitter_ids for a query
    """

    return db.get_cohort(query)


def download(cohort, output_dir):
    """Download the files for a given cohort

    Parameters
    ----------
    cohort : dataframe
        A df containing unique case_ids and their case_submitter_ids for a query
    output_dir : str
        The directory where the files should be downloaded

    Returns
    -------
    None
    """
    case_ids = cohort.index.tolist()
    case_submitter_ids = cohort.values.tolist()

    gdc_download = GDCFileDownloader(output_dir)
    gdc_download.process_cases(case_ids=case_ids, case_submitter_ids=case_submitter_ids)

    # idc_download = IDCFileDownloader(output_dir)
    # idc_download.process_cases(case_submitter_ids=case_submitter_ids)

    tcia_download = TCIAFileDownloader(output_dir)
    tcia_download.process_cases(case_submitter_ids=case_submitter_ids)


def update():
    """Update the database with the latest data from data sources

    Returns
    -------
    None
    """
    updater = MINDSUpdater()
    # updater.threaded_update()
    db.update(updater.temp_folder)
