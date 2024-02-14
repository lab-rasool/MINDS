import json
import os

import requests
from retry import retry
from tqdm.contrib.concurrent import thread_map


def numpy_to_python(data):
    """
    Recursively convert numpy data types to their native Python equivalents
    in the given data structure.
    """
    if isinstance(data, dict):
        return {key: numpy_to_python(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [numpy_to_python(item) for item in data]
    else:
        return data


class Aggregator:
    def __init__(self, cohort, output_dir, max_workers=4):
        self.cohort = cohort
        self.case_ids = self.cohort.index.to_list()
        self.case_submitter_ids = self.cohort.values.tolist()
        self.DATA_DIR = output_dir
        self.MAX_WORKERS = max_workers
        self.BASE_URL = "https://api.gdc.cancer.gov/"
        self.FILES_ENDPOINT = "files"

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def gdc_files(self, case_id, case_submitter_id):
        fields = [
            "access",
            "created_datetime",
            "data_category",
            "data_format",
            "data_type",
            "experimental_strategy",
            "file_name",
            "file_size",
            "file_state",
            "md5sum",
            "origin",
            "platform",
            "revision",
            "state",
            "tags",
            "type",
            "updated_datetime",
        ]
        fields = ",".join(fields)
        filters = {
            "op": "and",
            "content": [
                {
                    "op": "in",
                    "content": {"field": "cases.case_id", "value": [case_id]},
                },
                {"op": "=", "content": {"field": "access", "value": ["open"]}},
            ],
        }
        params = {
            "filters": json.dumps(filters),
            "fields": fields,
            "format": "JSON",
            "size": "1_000_000",
        }
        response = requests.get(self.BASE_URL + self.FILES_ENDPOINT, params=params)
        data = response.json()["data"]["hits"]

        # Organize files by data_type
        organized_data = {"PatientID": case_submitter_id[0], "gdc_case_id": case_id}
        for file in data:
            data_type = file["data_type"]
            if data_type not in organized_data:
                organized_data[data_type] = []
            organized_data[data_type].append(file)
        return organized_data

    def generate_manifest(self):
        responses = thread_map(
            self.gdc_files,
            self.case_ids,
            self.case_submitter_ids,
            max_workers=self.MAX_WORKERS,
        )
        responses = numpy_to_python(
            responses
        )  # Ensure data is in a serializable format
        manifest_path = os.path.join(self.DATA_DIR, "manifest.json")
        with open(manifest_path, "w") as file:
            json.dump(responses, file, indent=4)
