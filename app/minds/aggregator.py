import json
import os
from collections import defaultdict

import requests
from retry import retry
from tqdm import tqdm
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
        self.GDC_BASE_URL = "https://api.gdc.cancer.gov/"
        self.TCIA_BASE_URL = (
            "https://services.cancerimagingarchive.net/nbia-api/services/v1/"
        )

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
        response = requests.get(self.GDC_BASE_URL + "files", params=params)
        data = response.json()["data"]["hits"]

        # Organize files by data_type
        organized_data = {"PatientID": case_submitter_id[0], "gdc_case_id": case_id}
        for file in data:
            data_type = file["data_type"]
            if data_type not in organized_data:
                organized_data[data_type] = []
            organized_data[data_type].append(file)
        return organized_data

    def transform_series_data(self, data):
        transformed_data = defaultdict(lambda: defaultdict(list))
        for item in data:
            patient_id = item["PatientID"]
            modality = item["Modality"]
            # Remove PatientID from nested data as it's used as a key
            nested_data = {k: v for k, v in item.items() if k != "PatientID"}
            # Append the dictionary to the list under the correct PatientID and Modality
            transformed_data[patient_id][modality].append(nested_data)
        # Convert defaultdict to a regular dict and format it to match the desired output structure
        final_data = []
        for patient_id, modalities in transformed_data.items():
            patient_dict = {"PatientID": patient_id}
            for modality, records in modalities.items():
                patient_dict[modality] = records
            final_data.append(patient_dict)
        return final_data

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def tcia_files(self, case_submitter_id):
        options = {}
        options["PatientID"] = case_submitter_id
        base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v4/"
        url = base_url + "getSeries"
        try:
            data = requests.get(url, params=options)
            data.raise_for_status()
            if data.text != "":
                data = data.json()
            else:
                return None
        except Exception as e:
            print(e)
            return None

        return data

    def append_tcia_to_manifest(self, manifest_path, tcia_responses):

        with open(manifest_path, "r") as file:
            manifest_data = json.load(file)

        for patient_data_list in tcia_responses:
            for new_data in patient_data_list:  # Corrected iteration
                patient_id = new_data["PatientID"]
                existing_patient = next(
                    (
                        item
                        for item in manifest_data
                        if item.get("PatientID") == patient_id
                    ),
                    None,
                )
                if existing_patient:
                    for modality, records in new_data.items():
                        if modality != "PatientID":
                            if modality in existing_patient:
                                existing_patient[modality].extend(records)
                            else:
                                existing_patient[modality] = records
                else:
                    manifest_data.append(new_data)

        # Write the updated manifest back to the file
        with open(manifest_path, "w") as file:
            json.dump(manifest_data, file, indent=4)

    def generate_manifest(self):
        manifest_path = os.path.join(self.DATA_DIR, "manifest.json")

        gdc_responses = thread_map(
            self.gdc_files,
            self.case_ids,
            self.case_submitter_ids,
            max_workers=self.MAX_WORKERS,
        )
        gdc_responses = numpy_to_python(
            gdc_responses
        )  # Ensure data is in a serializable format
        with open(manifest_path, "w") as file:
            json.dump(gdc_responses, file, indent=4)

        for case_submitter_id in tqdm(self.case_submitter_ids):
            tcia_responses = self.tcia_files(case_submitter_id)
            # tcia_responses = numpy_to_python(tcia_responses)
            if tcia_responses is not None:
                formatted_data = self.transform_series_data(tcia_responses)
                self.append_tcia_to_manifest(manifest_path, [formatted_data])
