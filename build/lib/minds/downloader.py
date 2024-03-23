import concurrent.futures
import io
import json
import logging
import os
import re
import shutil
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from urllib.parse import urlparse

import requests
from google.cloud import storage
from retry import retry
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress
from rich.traceback import install

logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)

install()

console = Console()


class GDCFileDownloader:
    def __init__(self, DATA_DIR, MAX_WORKERS, **kwargs):
        self.BASE_URL = "https://api.gdc.cancer.gov/"
        self.DATA_ENDPOINT = "data"
        self.DATA_DIR = DATA_DIR
        self.MAX_WORKERS = MAX_WORKERS
        self.MANIFEST_FILE = os.path.join(DATA_DIR, "manifest.json")
        with open(self.MANIFEST_FILE, "r") as f:
            self.manifest = json.load(f)
        self.include = kwargs.get("include", [])
        self.exclude = kwargs.get("exclude", [])

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def download_files(self, file_uuids, case_id):
        os.makedirs(self.DATA_DIR, exist_ok=True)
        try:
            response = requests.post(
                self.BASE_URL + self.DATA_ENDPOINT,
                data=json.dumps({"ids": [file_uuids]}),
                headers={"Content-Type": "application/json"},
            )
            response_head_cd = response.headers.get("Content-Disposition")

            if response_head_cd:
                file_name = re.findall("filename=(.+)", response_head_cd)[0]
                output_path = os.path.join(self.DATA_DIR, file_name)
                with open(output_path, "wb") as output_file:
                    output_file.write(response.content)
                logging.info(f"Downloaded {file_name} for case ID: {case_id}")
            else:
                logging.warning(
                    f"Content-Disposition header missing for case ID: {case_id}"
                )
        except Exception as e:
            logging.error(
                f"Failed to download files for case ID {case_id}: {e}", exc_info=True
            )

    def download_files_for_patient(self, patient_data):
        gdc_case_id = patient_data["gdc_case_id"]
        logging.info(f"Downloading files for PatientID: {gdc_case_id}")
        for data_type, files in patient_data.items():
            if data_type in ["PatientID", "gdc_case_id"]:
                continue

            if self.include and data_type not in self.include:
                continue

            if self.exclude and data_type in self.exclude:
                continue

            file_uuids = [file["id"] for file in files if "id" in file]
            self.download_files(file_uuids, gdc_case_id)

    def extract_files(self, ext, mode):
        for filename in os.listdir(self.DATA_DIR):
            if filename.endswith(ext):
                logging.info(f"Extracting {filename}")
                filepath = os.path.join(self.DATA_DIR, filename)
                try:
                    with tarfile.open(filepath, mode) as tar:
                        tar.extractall(filter="data", path=self.DATA_DIR)
                except EOFError as e:
                    logging.error(f"EOF Error: {e}. Check gdc.log for case_id details.")
                except PermissionError as e:
                    logging.error(f"Permission Error: {e}.")
                    continue
                except Exception as e:
                    logging.error(f"An error occurred while extracting {filename}: {e}")

    def organize_files(self):
        for patient in self.manifest:
            patient_id = patient["PatientID"]
            for data_type, files in patient.items():
                if data_type in ["PatientID", "gdc_case_id"]:  # Skip non-file entries
                    continue

                # skip if there is no "id"
                if not any("id" in file for file in files):
                    continue

                if self.include and data_type not in self.include:
                    continue

                if self.exclude and data_type in self.exclude:
                    continue

                for file in files:
                    file_uuid = file["id"]
                    file_name = file["file_name"]
                    # Construct the target directory path for each file
                    target_dir = os.path.join(
                        self.DATA_DIR, "raw", patient_id, data_type, file_uuid
                    )
                    os.makedirs(target_dir, exist_ok=True)

                    # Construct the source file path
                    source_file_path = os.path.join(self.DATA_DIR, file_name)

                    # Check if the source file exists and move it
                    if os.path.isfile(source_file_path):
                        shutil.move(source_file_path, target_dir)

                    # If a folder exists for the file UUID (not expected in JSON structure but just in case)
                    source_folder_path = os.path.join(self.DATA_DIR, file_uuid)
                    if os.path.isdir(source_folder_path):
                        for item in os.listdir(source_folder_path):
                            try:
                                shutil.move(
                                    os.path.join(source_folder_path, item), target_dir
                                )
                            except Exception as e:
                                logging.error(
                                    f"An error occurred while moving {item} to {target_dir}: {e}"
                                )
                        os.rmdir(
                            source_folder_path
                        )  # Remove the now-empty file_uuid directory

    def multi_download(self):
        """
        Concurrently download files for all patients in the manifest using rich progress bar.
        """
        with Progress() as progress:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                task = progress.add_task(
                    "Downloading files from GDC", total=len(self.manifest)
                )
                futures = {
                    executor.submit(
                        self.download_files_for_patient, patient_data
                    ): patient_data
                    for patient_data in self.manifest
                }
                # for patient_data in self.manifest:
                #     executor.submit(self.download_files_for_patient, patient_data)
                for future in as_completed(futures):
                    progress.update(task, advance=1)

    def multi_extract(self):
        """
        Concurrently extract all .gz and .tar files in the data directory.
        """
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for ext, mode in [(".tar.gz", "r:gz"), (".tar", "r:")]:
                executor.submit(self.extract_files, ext, mode)

    def post_process_cleanup(self):
        """
        Clean up the data directory by removing all .gz, .tar, .txt, and .log files.
        """
        for filename in os.listdir(self.DATA_DIR):
            if (
                filename.endswith(".tar.gz")
                or filename.endswith(".tar")
                or filename.endswith(".log")
                or filename.endswith(".txt")
            ):
                filepath = os.path.join(self.DATA_DIR, filename)
                os.remove(filepath)

    def process_cases(self):
        """
        Process a list of case_ids by downloading, extracting, organizing, and cleaning up files.

        :param case_ids: List of case IDs to process.
        """
        self.multi_download()
        self.multi_extract()
        self.organize_files()
        self.post_process_cleanup()


class TCIAFileDownloader:
    def __init__(self, output_dir, MAX_WORKERS, **kwargs):
        self.output_dir = output_dir
        self.MAX_WORKERS = MAX_WORKERS
        self.MANIFEST_FILE = os.path.join(output_dir, "manifest.json")
        self.include = kwargs.get("include", [])
        self.exclude = kwargs.get("exclude", [])

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def downloadSeries(
        self,
        series_data,
        number=0,
        path="",
    ):
        seriesUID = ""
        success = 0
        base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v1/"
        downloadOptions = "getImage?NewFileNames=Yes&SeriesInstanceUID="

        with Progress() as progress:
            try:
                task = progress.add_task(
                    "Downloading series from TCIA", total=len(series_data)
                )
                for x in series_data:
                    seriesUID = x
                    pathTmp = path + "/" + seriesUID
                    data_url = base_url + downloadOptions + seriesUID
                    if not os.path.isdir(pathTmp):
                        data = requests.get(data_url)
                        if data.status_code == 200:
                            file = zipfile.ZipFile(io.BytesIO(data.content))
                            file.extractall(path=pathTmp)
                            success += 1
                            if number > 0:
                                if success == number:
                                    break
                    progress.update(task, advance=1)
            except Exception as e:
                logging.error(f"Failed to download series {seriesUID}: {e}")

    # Function to recursively search for keys in nested dictionaries and lists
    def find_values(self, key, dictionary):
        found_values = []

        if isinstance(dictionary, dict):
            for k, v in dictionary.items():
                if k == key:
                    found_values.append(v)
                elif isinstance(v, (dict, list)):
                    found_values.extend(self.find_values(key, v))

        elif isinstance(dictionary, list):
            for item in dictionary:
                found_values.extend(self.find_values(key, item))

        return found_values

    def find_and_process_series(self):
        with open(self.MANIFEST_FILE, "r") as file:
            manifest = json.load(file)

        modalities = [
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

        for entry in manifest:
            patient_id = entry.get("PatientID")
            for modality in modalities:
                if modality in entry:
                    # Process each series under the modality
                    for series in entry[modality]:
                        series_instance_uid = series.get("SeriesInstanceUID")
                        if series_instance_uid:
                            self.move_series_folder(
                                series_instance_uid, patient_id, modality
                            )

    def move_series_folder(self, series_instance_uid, patient_id, modality):
        source_path = os.path.join(self.output_dir, series_instance_uid)
        dest_path = os.path.join(
            self.output_dir, "raw", patient_id, modality, series_instance_uid
        )

        if not os.path.exists(source_path):
            logging.warning(f"Series not found: {series_instance_uid}")
            return

        os.makedirs(dest_path, exist_ok=True)
        # Ensure the destination directory does not already contain a directory with the same name
        if os.path.exists(os.path.join(dest_path, series_instance_uid)):
            logging.warning(f"Series already exists: {series_instance_uid}")
        else:
            shutil.move(source_path, dest_path)

    def process_cases(self):
        with open(self.MANIFEST_FILE, "r") as f:
            manifest = json.load(f)
        series_instance_uids = self.find_values("SeriesInstanceUID", manifest)
        self.downloadSeries(series_instance_uids, path=self.output_dir)
        self.find_and_process_series()


class IDCFileDownloader:
    def __init__(self, save_directory):
        self.idc_api_preamble = "https://api.imaging.datacommons.cancer.gov/v1"
        self.save_directory = save_directory

    @retry(tries=5, delay=2, backoff=2)
    def make_api_call(self, url, params, body):
        response = requests.post(url, params=params, json=body)
        if response.status_code == 200:
            data = response.json()
            if "manifest" not in data:
                return data
            totalFound = data["manifest"]["totalFound"]
            rowsReturned = data["manifest"]["rowsReturned"]
            if totalFound <= rowsReturned:
                return data
            else:
                params["page_size"] = totalFound + 10
                return self.make_api_call(url, params, body)
        else:
            raise Exception(f"Request failed: {response.reason}")

    def get_manifest_preview(self, filters):
        url = f"{self.idc_api_preamble}/cohorts/manifest/preview"
        params = dict(
            sql=False,
            Collection_ID=True,
            Patient_ID=True,
            StudyInstanceUID=True,
            SeriesInstanceUID=True,
            SOPInstanceUID=True,
            Source_DOI=True,
            CRDC_Study_GUID=True,
            CRDC_Series_GUID=True,
            CRDC_Instance_GUID=True,
            GCS_URL=True,
        )
        body = {
            "name": "MINDS",
            "description": "MINDS",
            "filters": filters,
        }
        return self.make_api_call(url, params, body)

    def get_query_preview(self, filters):
        url = f"{self.idc_api_preamble}/cohorts/query/preview"
        params = dict(
            sql=False,
        )
        cohort_def = {
            "name": "testcohort",
            "description": "Test description",
            "filters": filters,
        }
        queryFields = {
            "fields": [
                "Modality",
                "BodyPartExamined",
                "StudyDescription",
                "StudyInstanceUID",
                "PatientID",
                "Program",
                "SeriesInstanceUID",
                "SOPInstanceUID",
                "SeriesDescription",
                "SliceThickness",
                "SeriesNumber",
                "StudyDate",
                "SOPClassUID",
                "collection_id",
                "AnatomicRegionSequence",
                "SegmentedPropertyCategoryCodeSequence",
                "SegmentedPropertyTypeCodeSequence",
                "FrameOfReferenceUID",
                "SegmentNumber",
                "SegmentAlgorithmType",
                "SUVbw",
                "Volume",
                "Diameter",
                "Surface_area_of",
                "Total_Lesion_Glycolysis",
                "Standardized_Added_Metabolic_Activity",
                "Percent_Within_First_Quarter_of_Intensity_Range",
                "Percent_Within_Third_Quarter_of_Intensity_Range",
                "Percent_Within_Fourth_Quarter_of_Intensity_Range",
                "Percent_Within_Second_Quarter_of_Intensity_Range",
                "Standardized_Added_Metabolic_Activity_Background",
                "Glycolysis_Within_First_Quarter_of_Intensity_Range",
                "Glycolysis_Within_Third_Quarter_of_Intensity_Range",
                "Glycolysis_Within_Fourth_Quarter_of_Intensity_Range",
                "Glycolysis_Within_Second_Quarter_of_Intensity_Range",
                "Internal_structure",
                "Sphericity",
                "Calcification",
                "Lobular_Pattern",
                "Spiculation",
                "Margin",
                "Texture",
                "Subtlety_score",
                "Malignancy",
                "Apparent_Diffusion_Coefficient",
                "tcia_species",
                "Manufacturer",
                "ManufacturerModelName",
                "license_short_name",
            ]
        }
        body = {
            "cohort_def": cohort_def,
            "queryFields": queryFields,
        }
        return self.make_api_call(url, params, body)

    def merge_data(self, manifest_data, query_data):
        merged_data = []

        # Create dictionaries to hold lists of entries for each Patient_ID
        manifest_dict = {}
        query_dict = {}

        # Populate manifest_dict
        for manifest_entry in manifest_data.get("manifest", {}).get(
            "json_manifest", []
        ):
            patient_id = manifest_entry.get("Patient_ID")
            if patient_id not in manifest_dict:
                manifest_dict[patient_id] = []
            manifest_dict[patient_id].append(manifest_entry)

        # Populate query_dict
        for query_entry in query_data.get("query_results", {}).get("json", []):
            patient_id = query_entry.get("PatientID")
            if patient_id not in query_dict:
                query_dict[patient_id] = []
            query_dict[patient_id].append(query_entry)

        # Merge the data
        for patient_id, manifest_entries in manifest_dict.items():
            if patient_id in query_dict:
                query_entries = query_dict[patient_id]
                for manifest_entry in manifest_entries:
                    for query_entry in query_entries:
                        merged_entry = {
                            "Patient_ID": patient_id,
                            "GCS_URL": manifest_entry.get("GCS_URL"),
                            "Modality": query_entry.get("Modality"),
                        }
                        if merged_entry not in merged_data:
                            merged_data.append(merged_entry)
        return merged_data

    def download_dicom_files(self, merged_data):
        client = storage.Client.create_anonymous_client()
        aggregated_data = {}

        # Step 1: Aggregate all patient data
        for patient_data in merged_data:
            patient_id = patient_data["Patient_ID"]
            gcs_url = patient_data["GCS_URL"]
            modality = patient_data["Modality"]
            # Skip over "SM" modalities
            if modality == "SM":
                continue
            file_info = {"gcs_url": gcs_url, "modality": modality}

            if patient_id in aggregated_data:
                aggregated_data[patient_id].append(file_info)
            else:
                aggregated_data[patient_id] = [file_info]

        # Step 2: Download DICOM files
        def download_single_file(file_info, patient_id):
            gcs_url = file_info["gcs_url"]
            modality = file_info["modality"]

            parsed_url = urlparse(gcs_url)
            bucket_name = parsed_url.netloc
            blob_name = parsed_url.path.lstrip("/")
            save_path = os.path.join(
                self.save_directory, "raw", patient_id, modality, blob_name
            )
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename(save_path)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for patient_id, file_infos in aggregated_data.items():
                for file_info in file_infos:
                    executor.submit(download_single_file, file_info, patient_id)

    def update_manifest(self, merged_data):
        manifest_path = os.path.join(self.save_directory, "manifest.json")

        # Read existing manifest
        with open(manifest_path, "r") as f:
            manifest_data = json.load(f) if os.path.exists(manifest_path) else []

        # Create a dictionary for faster look-up
        manifest_dict = {item.get("case_id"): item for item in manifest_data}

        # Function to update a single manifest entry
        def update_single_entry(patient_data):
            patient_id = patient_data["Patient_ID"]
            modality = patient_data["Modality"]
            gcs_url = patient_data["GCS_URL"]
            folder_name = urlparse(gcs_url).path.strip("/").split("/")[-2]

            if modality == "SM":
                return

            if patient_id in manifest_dict:
                manifest_entry = manifest_dict[patient_id]
                if modality not in manifest_entry:
                    manifest_entry[modality] = []
                if folder_name not in manifest_entry[modality]:
                    manifest_entry[modality].append(folder_name)
            else:
                new_entry = {"case_id": patient_id, modality: [folder_name]}
                manifest_data.append(new_entry)
                manifest_dict[patient_id] = new_entry

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.MAX_WORKERS
        ) as executor:
            for patient_data in merged_data:
                executor.submit(update_single_entry, patient_data)

        # Save the updated manifest back to disk
        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f, indent=4)

    def generate_merged_data_for_case(self, single_case_submitter_id):
        filters = {"PatientID": [single_case_submitter_id[0]]}
        manifest_data = self.get_manifest_preview(filters)
        query_data = self.get_query_preview(filters)

        if manifest_data and query_data:
            return self.merge_data(manifest_data, query_data)
        return None

    def process_cases(self, case_submitter_ids):
        # Generate merged_data for all cases
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            all_merged_data = list(
                executor.map(self.generate_merged_data_for_case, case_submitter_ids)
            )

        all_merged_data = list(chain.from_iterable(all_merged_data))

        self.download_dicom_files(all_merged_data)
        self.update_manifest(all_merged_data)
