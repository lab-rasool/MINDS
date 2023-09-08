from .downloader import GDCFileDownloader
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
        """Query the database and return the case ids as a list

        Parameters
        ----------
        query_string : str
            The query string to be executed on the database

        Returns
        -------
        list
            The list of case ids
        """
        return self.db.get_cohort(query_string)
