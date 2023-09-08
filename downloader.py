import os
import re
import requests
import json
import tarfile
import shutil
import time
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map


class GDCFileDownloader:
    """
    Class for downloading files from the GDC API based on case_ids.
    """

    def __init__(self, DATA_DIR):
        """
        Initialize the downloader with a specific data directory.

        :param DATA_DIR: Directory where downloaded data will be stored.
        """
        self.BASE_URL = "https://api.gdc.cancer.gov/"
        self.FILES_ENDPOINT = "files"
        self.DATA_ENDPOINT = "data"
        self.DATA_DIR = DATA_DIR

    def get_file_uuids_for_case_id(self, case_id):
        """
        Fetch file UUIDs from the GDC API based on a given case_id.

        :param case_id: The ID of the case to fetch file UUIDs for.
        :return: List of file UUIDs associated with the given case_id.
        """
        params = {
            "filters": json.dumps(
                {
                    "op": "and",
                    "content": [
                        {
                            "op": "=",
                            "content": {"field": "cases.case_id", "value": [case_id]},
                        },
                        {"op": "=", "content": {"field": "access", "value": ["open"]}},
                    ],
                }
            ),
            "fields": "file_id",
            "format": "JSON",
            "size": "1_000_000",
        }
        response = requests.get(self.BASE_URL + self.FILES_ENDPOINT, params=params)
        return [entry["file_id"] for entry in response.json()["data"]["hits"]]

    def download_files_for_case_id(self, case_id):
        """
        Download all files associated with a given case_id.

        :param case_id: The ID of the case to download files for.
        """
        file_uuid_list = self.get_file_uuids_for_case_id(case_id)
        response = requests.post(
            self.BASE_URL + self.DATA_ENDPOINT,
            data=json.dumps({"ids": file_uuid_list}),
            headers={"Content-Type": "application/json"},
        )
        file_name = re.findall(
            "filename=(.+)", response.headers["Content-Disposition"]
        )[0]
        file_extension = file_name.split(".")[-1]

        os.makedirs(self.DATA_DIR, exist_ok=True)

        output_path = os.path.join(self.DATA_DIR, f"{case_id}.{file_extension}")
        with open(output_path, "wb") as output_file:
            output_file.write(response.content)

    def extract_files(self, ext, mode):
        """
        Extract files with a given extension from the data directory.

        :param ext: The file extension to look for.
        :param mode: The mode to use when opening the tarfile.
        """
        for filename in os.listdir(self.DATA_DIR):
            if filename.endswith(ext):
                filepath = os.path.join(self.DATA_DIR, filename)
                with tarfile.open(filepath, mode) as tar:
                    try:
                        tar.extractall(path=self.DATA_DIR)
                    except FileExistsError:
                        pass

    def organize_files(self, case_id):
        """
        Organize files in the data directory into subdirectories by case_id and data type.

        :param case_id: The ID of the case to organize files for.
        """
        target_dir = os.path.join(self.DATA_DIR, "raw", case_id)
        for file_uuid in self.get_file_uuids_for_case_id(case_id):
            response = requests.get(
                self.BASE_URL + self.FILES_ENDPOINT + "/" + file_uuid
            )
            if response.status_code == 200:
                data_type = response.json()["data"]["data_type"]
                os.makedirs(os.path.join(target_dir, data_type), exist_ok=True)
                try:
                    shutil.move(
                        os.path.join(self.DATA_DIR, file_uuid),
                        os.path.join(target_dir, data_type, file_uuid),
                    )
                except (FileNotFoundError, FileExistsError, shutil.Error):
                    pass
            elif response.status_code == 429:
                time.sleep(30)
                self.organize_files(case_id)

    def generate_manifest(self):
        """
        Generate a manifest.json file in the data directory that logs all files in the /raw subdirectory.
        """
        manifest = []
        raw_dir = os.path.join(self.DATA_DIR, "raw")
        for case_id in os.listdir(raw_dir):
            case_dir = os.path.join(raw_dir, case_id)
            if not os.path.isdir(case_dir):
                continue
            case_manifest = {"case_id": case_id}
            for data_type in os.listdir(case_dir):
                data_type_dir = os.path.join(case_dir, data_type)
                data_manifest = []
                for filename in os.listdir(data_type_dir):
                    file_uuid = os.path.splitext(filename)[0]
                    data_manifest.append(file_uuid)
                case_manifest[data_type] = data_manifest
            manifest.append(case_manifest)
        with open(os.path.join(self.DATA_DIR, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=4)

    def post_process_cleanup(self):
        """
        Clean up the data directory by removing all .gz, .tar, and .txt files.
        """
        for filename in tqdm(os.listdir(self.DATA_DIR)):
            if (
                filename.endswith(".gz")
                or filename.endswith(".tar")
                or filename.endswith(".txt")
            ):
                filepath = os.path.join(self.DATA_DIR, filename)
                os.remove(filepath)

    def multi_download(self, case_ids):
        """
        Concurrently download files for multiple case_ids.

        :param case_ids: List of case IDs to download files for.
        """
        thread_map(self.download_files_for_case_id, case_ids)

    def multi_extract(self):
        """
        Concurrently extract all .gz and .tar files in the data directory.
        """
        thread_map(
            lambda ext, mode: self.extract_files(ext, mode),
            [".gz", ".tar"],
            ["r:gz", "r"],
        )

    def multi_organize(self, case_ids):
        """
        Concurrently organize files for multiple case_ids.

        :param case_ids: List of case IDs to organize files for.
        """
        thread_map(self.organize_files, case_ids)

    def process_cases(self, case_ids):
        """
        Process a list of case_ids by downloading, extracting, organizing, and cleaning up files.

        :param case_ids: List of case IDs to process.
        """
        self.multi_download(case_ids)
        self.multi_extract()
        self.multi_organize(case_ids)
        self.post_process_cleanup()
        self.generate_manifest()


class IDCFileDownloader:
    def __init__(self, DATA_DIR):
        """_summary_

        Args:
            DATA_DIR (_type_): _description_
        """
        pass
