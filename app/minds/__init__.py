import json
import os

from rich.console import Console
from rich.table import Table

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

    def download(self, threads: int = 4, include: list = None, exclude: list = None):
        if not os.path.exists(self.manifest_file):
            raise FileNotFoundError(
                f"No manifest file found in {self.output_dir}. Please run generate_manifest first."
            )

        self._download_gdc_files(threads, include=include, exclude=exclude)
        self._download_tcia_files(threads)

    def include(self, modalities):
        print(f"Only including {modalities} modalities in download")

    def stats(self):
        """Prints the statistics of the cohort in terms of file count and total size

        Returns:
            dict: A dictionary containing the statistics of the cohort
        """
        with open(self.manifest_file, "r") as f:
            manifest = json.load(f)

        stats_dict = {}
        for entry in manifest:
            for key, value in entry.items():
                if isinstance(value, list):
                    patient_size = 0
                    for file in value:
                        try:
                            patient_size += file["file_size"]
                        except Exception as e:
                            pass

                    if key not in stats_dict:
                        stats_dict[key] = {
                            "file_count": len(value),
                            "total_size": patient_size,
                        }
                    else:
                        stats_dict[key]["file_count"] += len(value)
                        stats_dict[key]["total_size"] += patient_size

        # Sort the dictionary by total size in descending order
        sorted_stats = sorted(
            stats_dict.items(), key=lambda x: x[1]["total_size"], reverse=True
        )

        console = Console()
        table = Table(show_header=True, header_style="bold green")
        table.add_column("Modality")
        table.add_column("File Count")
        table.add_column("Total Size")

        for key, value in sorted_stats:
            size = value["total_size"]
            if size > 1024 * 1024 * 1024:
                size = f"{size / (1024 * 1024 * 1024):.2f} GB"
            elif size > 1024 * 1024:
                size = f"{size / (1024 * 1024):.2f} MB"
            else:
                size = f"{size / 1024:.2f} KB"
            table.add_row(key, str(value["file_count"]), size)

        console.print(table)
        return dict(sorted_stats)

    def _download_gdc_files(self, threads, include=None, exclude=None):
        gdc_downloader = GDCFileDownloader(
            self.output_dir, MAX_WORKERS=threads, include=include, exclude=exclude
        )
        gdc_downloader.process_cases()

    def _download_tcia_files(self, threads, include=None, exclude=None):
        tcia_downloader = TCIAFileDownloader(
            self.output_dir, MAX_WORKERS=threads, include=include, exclude=exclude
        )
        tcia_downloader.process_cases()


def build_cohort(output_dir, query=None, gdc_cohort=None, manifest=None):
    """Builds a cohort based on a query or a GDC cohort file and returns a Cohort object."""
    if query:
        cohort_data = db.get_minds_cohort(query)
    elif gdc_cohort:
        cohort_data = db.get_gdc_cohort(gdc_cohort)
    else:
        raise ValueError("Either a query or a gdc_cohort file must be provided")

    cohort = Cohort(cohort_data, output_dir)
    if manifest is None:
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
