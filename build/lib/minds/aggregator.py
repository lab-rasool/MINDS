import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from retry import retry
from rich.progress import Progress


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
    def __init__(self, cohort, output_dir, max_workers=6):
        self.cohort = cohort
        self.case_ids = self.cohort.index.to_list()
        self.case_submitter_ids = self.cohort.values.tolist()
        self.DATA_DIR = output_dir
        self.MAX_WORKERS = max_workers
        self.GDC_BASE_URL = "https://api.gdc.cancer.gov/"
        self.TCIA_BASE_URL = (
            "https://services.cancerimagingarchive.net/services/v4/TCIA/query/"
        )
        self.manifest_path = os.path.join(self.DATA_DIR, "manifest.json")
        self.structured_manifest_all_modalities = []

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

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def tcia_files(self, patient_id):
        output_format = "&format=json"
        GET_SERIES_BY_PATIENT_ID = "getSeries?PatientID="
        url = self.TCIA_BASE_URL + GET_SERIES_BY_PATIENT_ID + patient_id + output_format
        url = url[0]
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Error in TCIA request: {e}")
            return None

    def add_or_update_entry_all_modalities(self, patient_id, modality_data, modality):
        for entry in self.structured_manifest_all_modalities:
            if entry["PatientID"] == patient_id:
                if modality not in entry:
                    entry[modality] = [modality_data]
                else:
                    entry[modality].append(modality_data)
                return
        self.structured_manifest_all_modalities.append(
            {"PatientID": patient_id, modality: [modality_data]}
        )

    def format_tcia_response(self, tcia_response):
        tcia_modalities = [
            "MG",
            "MR",
            "CT",
            "SEG",
            "RTSTRUCT",
            "CR",
            "SR",
            "US",
            "PT",
            "DX",
            "RTDOSE",
            "RTPLAN",
            "PR",
            "REG",
            "RWV",
            "NM",
            "KO",
            "FUSION",
            "OT",
            "XA",
            "SC",
            "RF",
        ]

        for patient_data in tcia_response:
            for item in patient_data:
                modality = item.get("Modality")
                if modality in tcia_modalities:
                    modality_data = {
                        k: item[k] for k in item if k != "PatientID" and k != "Modality"
                    }
                    self.add_or_update_entry_all_modalities(
                        item["PatientID"], modality_data, modality
                    )

    def generate_manifest(self):
        with Progress() as progress:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                task = progress.add_task(
                    "Generating manifest...", total=len(self.case_ids)
                )
                futures = {
                    executor.submit(self.gdc_files, case_id, case_submitter_id): case_id
                    for case_id, case_submitter_id in zip(
                        self.case_ids, self.case_submitter_ids
                    )
                }

                all_gdc_responses = []
                for future in as_completed(futures):
                    progress.update(task, advance=1)
                    gdc_response = future.result()
                    all_gdc_responses.append(gdc_response)

        processed_responses = [
            numpy_to_python(response) for response in all_gdc_responses
        ]

        with open(self.manifest_path, "w") as f:
            json.dump(processed_responses, f, indent=4)

        with Progress() as progress:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                task = progress.add_task(
                    "Aggregating data from other sources...",
                    total=len(self.case_submitter_ids),
                )
                futures = {
                    executor.submit(self.tcia_files, patient_id): patient_id
                    for patient_id in self.case_submitter_ids
                }

                all_tcia_responses = []
                for future in as_completed(futures):
                    progress.update(task, advance=1)
                    tcia_response = future.result()
                    all_tcia_responses.append(tcia_response)

        self.format_tcia_response(all_tcia_responses)

        with open(self.manifest_path, "r") as f:
            existing_manifest = json.load(f)

        for entry in self.structured_manifest_all_modalities:
            for existing_entry in existing_manifest:
                if entry["PatientID"] == existing_entry["PatientID"]:
                    existing_entry.update(entry)
                    break
            else:
                existing_manifest.append(entry)

        with open(self.manifest_path, "w") as f:
            json.dump(existing_manifest, f, indent=4)
