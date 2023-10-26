from .downloader import GDCFileDownloader, IDCFileDownloader
from .database import DatabaseManager


class MINDS:
    def __init__(self):
        self.db = DatabaseManager()

    def query(self, query_string):
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
        return self.db.execute(query_string)

    def get_cohort(self, query_string):
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
        return self.db.get_cohort(query_string)

    def download(self, cohort, output_dir):
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
        gdc_download.process_cases(
            case_ids=case_ids, case_submitter_ids=case_submitter_ids
        )

        idc_download = IDCFileDownloader(output_dir)
        idc_download.process_cases(case_submitter_ids=case_submitter_ids)
