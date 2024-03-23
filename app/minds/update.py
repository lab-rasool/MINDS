import datetime
import logging
import os
import tarfile
from concurrent.futures import ThreadPoolExecutor

import requests
import retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MINDSUpdater:
    def __init__(self):
        self.CLINICAL_URL = "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar"
        self.BIOSPECIMEN_URL = (
            "https://portal.gdc.cancer.gov/auth/api/v0/biospecimen_tar"
        )
        self.session = requests.Session()
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.clinical_tar_file = f"clinical.cases_selection.{self.today}.tar.gz"
        self.biospecimen_tar_file = f"biospecimen.cases_selection.{self.today}.tar.gz"
        self.temp_folder = os.getcwd() + "/tmp"

    def build_data(self):
        size = 100_000  # TODO: get this number from the GDC API
        filters = '{"op":"and","content":[{"op":"in","content":{"field":"files.access","value":["open"]}}]}'
        return {
            "size": size,
            "attachment": True,
            "format": "TSV",
            "filters": filters,
        }

    @retry.retry(tries=3, delay=2, backoff=2, jitter=(1, 3))
    def download(self, url):
        data = self.build_data()
        data_type = url.split("/")[-1].split("_")[0]
        file_name = f"{data_type}.cases_selection.{self.today}.tar.gz"
        if os.path.exists(file_name):
            logging.info(f"The file {file_name} already exists. Skipping download.")
            return
        try:
            logging.info(f"Downloading {data_type} data...")
            response = self.session.post(url, data=data, stream=True)
            response.raise_for_status()
            chunk_size = 1024
            with open(file_name, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
            logging.info("File downloaded successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download the file: {e}")

    def extract(self):
        tar_file = tarfile.open(self.clinical_tar_file)
        tar_file.extractall(self.temp_folder)
        tar_file.close()
        tar_file = tarfile.open(self.biospecimen_tar_file)
        tar_file.extractall(self.temp_folder)
        tar_file.close()
        os.remove(self.clinical_tar_file)
        os.remove(self.biospecimen_tar_file)

    def temp_folder(self):
        return self.temp_folder

    def threaded_update(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.download, self.CLINICAL_URL)
            executor.submit(self.download, self.BIOSPECIMEN_URL)
        self.extract()
