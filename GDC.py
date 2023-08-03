import os
import requests
import json
import tarfile
import shutil
import time
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map


class GDCFileDownloader:
    def __init__(self, directory, case_ids):
        self.directory = directory
        self.case_ids = case_ids
        self.failed_cases_ids = []

    def get_file_uuids(self, case_id):
        file_uuids = []
        endpoint = "https://api.gdc.cancer.gov/files"
        filters = {
            "op": "and",
            "content": [
                {"op": "=", "content": {"field": "cases.case_id", "value": [case_id]}},
                {"op": "=", "content": {"field": "access", "value": ["open"]}},
            ],
        }
        params = {
            "filters": json.dumps(filters),
            "fields": "file_id",
            "format": "json",
            "size": "1_000_000",  # Adjust this number as needed
        }
        time.sleep(15)
        response = requests.get(endpoint, params=params)  # Send the GET request
        if response.status_code == 200:  # Check if the request was successful
            data = response.json()  # Parse the JSON response
            file_uuids.extend([file["file_id"] for file in data["data"]["hits"]])
            if data["warnings"]:
                print(f"Warnings: {data['warnings']}")
        elif response.status_code == 429:
            print("Too many requests, waiting 30 seconds")
            time.sleep(30)
            self.get_file_uuids(case_id)
        return file_uuids

    def download_files(self, case_id):
        file_uuids = self.get_file_uuids(case_id)
        file_path = os.path.join(self.directory, f"{case_id}.tar.gz")
        if os.path.exists(file_path):
            return
        url = f"https://api.gdc.cancer.gov/data/{','.join(file_uuids)}"
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size_in_bytes = int(response.headers.get("content-length", 0))
            progress_bar = tqdm(
                total=total_size_in_bytes,
                unit="iB",
                unit_scale=True,
                desc=f"Downloading {case_id}",
                leave=False,
            )
            with open(file_path, "wb") as f:
                for data in response.iter_content(chunk_size=1024):
                    progress_bar.update(len(data))
                    f.write(data)
            progress_bar.close()
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                print("ERROR, something went wrong")
        elif response.status_code == 429:
            print("Too many requests, waiting 30 seconds")
            time.sleep(30)
            self.download_files(case_id)

    def organize_files(self, case_id):
        base_url = "https://api.gdc.cancer.gov/"
        endpoint = "files"
        file_uuids = self.get_file_uuids(case_id)
        source_dir = self.directory
        target_dir = self.directory + "/raw/" + case_id
        for file_uuid in file_uuids:
            response = requests.get(base_url + endpoint + "/" + file_uuid)
            if response.status_code == 200:
                data = response.json()
                data_type = data["data"]["data_type"]
                os.makedirs(target_dir + "/" + data_type, exist_ok=True)
                try:
                    shutil.move(
                        source_dir + "/" + file_uuid,
                        target_dir + "/" + data_type + "/" + file_uuid,
                    )
                    print(f"Moving {file_uuid}")
                except (FileNotFoundError, FileExistsError, shutil.Error):
                    pass
            elif response.status_code == 429:
                time.sleep(30)
                self.organize_files(case_id)

    def run(self):
        os.makedirs(self.directory + "/raw", exist_ok=True)
        thread_map(
            self.download_files, self.case_ids, desc="Downloading files", leave=True
        )
        # for case_id in tqdm(self.case_ids, desc="Extracting files", leave=True):
        #     self.extract_tar(case_id)
        thread_map(self.extract_tar, self.case_ids, desc="Extracting files", leave=True)
        print(f"Failed to extract {len(self.failed_cases_ids)} cases")
        thread_map(
            self.organize_files, self.case_ids, desc="Organizing files", leave=True
        )

    def extract_tar(self, case_id):
        try:
            tar = tarfile.open(os.path.join(self.directory, f"{case_id}.tar.gz"))
            tar.extractall(path=self.directory)
            tar.close()
            self.delete_tar(case_id)
        except:
            self.failed_cases_ids.append(case_id)
            pass

    def delete_tar(self, case_id):
        tar_path = os.path.join(self.directory, f"{case_id}.tar.gz")
        if os.path.exists(tar_path):  # Check if the .tar.gz file exists
            os.remove(tar_path)  # Delete the .tar.gz file

    def cleanup(self, case_id, file_uuids):
        os.remove(self.directory + "/MANIFEST.txt")
        for file_uuid in file_uuids:
            file_path = self.directory + "/" + file_uuid
            if os.path.exists(file_path):  # Check if the file exists
                os.remove(file_path)
