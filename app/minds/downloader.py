import concurrent.futures
import io
import json
import logging
import os
import re
import shutil
import tarfile
import time
import zipfile
from datetime import datetime, timedelta
from itertools import chain
from urllib.parse import urlparse

import pandas as pd
import requests
from google.cloud import storage
from retry import retry
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class PostProcessor:
    def __init__(self, DATA_DIR, case_ids, case_submitter_ids):
        self.DATA_DIR = DATA_DIR
        self.case_ids = case_ids
        self.case_submitter_ids = case_submitter_ids

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
                    data_manifest.append(filename)
                case_manifest[data_type] = data_manifest
            manifest.append(case_manifest)
        with open(os.path.join(self.DATA_DIR, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=4)

    def rename_files(self):
        raw_data_path = os.path.join(self.DATA_DIR, "raw")
        # Create a mapping of case_ids to their corresponding case_submitter_ids
        case_mapping = dict(
            zip(self.case_ids, [str(x[0]) for x in self.case_submitter_ids])
        )
        # Renaming directories
        for case_id, case_submitter_id in case_mapping.items():
            case_id_path = os.path.join(raw_data_path, case_id)
            case_submitter_id_path = os.path.join(raw_data_path, case_submitter_id)
            if os.path.exists(case_submitter_id_path) and os.listdir(
                case_submitter_id_path
            ):
                logging.info(
                    f"Directory {case_submitter_id_path} already exists. Skipping rename."
                )
                for data_type in os.listdir(case_id_path):
                    data_type_path = os.path.join(case_id_path, data_type)
                    case_submitter_id_data_type_path = os.path.join(
                        case_submitter_id_path, data_type
                    )
                    os.makedirs(case_submitter_id_data_type_path, exist_ok=True)
                    for filename in os.listdir(data_type_path):
                        file_path = os.path.join(data_type_path, filename)
                        if os.path.exists(
                            os.path.join(case_submitter_id_data_type_path, filename)
                        ):
                            case_id_data_type_path = os.path.join(
                                case_id_path, data_type, filename
                            )
                            try:
                                shutil.rmtree(case_id_data_type_path)
                            except Exception as e:
                                logging.error(
                                    f"An error occurred while deleting {case_id_path}: {e}"
                                )
                            continue
                        else:
                            shutil.move(file_path, case_submitter_id_data_type_path)
                    os.rmdir(data_type_path)
                try:
                    os.rmdir(case_id_path)
                except Exception as e:
                    logging.error(
                        f"An error occurred while deleting {case_id_path}: {e}"
                    )
            else:
                os.rename(case_id_path, case_submitter_id_path)


class GDCFileDownloader:
    """
    Class for downloading files from the GDC API based on case_ids.
    """

    def __init__(self, DATA_DIR, MAX_WORKERS):
        """
        Initialize the downloader with a specific data directory.

        :param DATA_DIR: Directory where downloaded data will be stored.
        """
        self.BASE_URL = "https://api.gdc.cancer.gov/"
        self.FILES_ENDPOINT = "files"
        self.DATA_ENDPOINT = "data"
        self.DATA_DIR = DATA_DIR
        self.MAX_WORKERS = MAX_WORKERS

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def get_file_uuids_for_case_id(self, case_id):
        """
        Fetch file UUIDs from the GDC API based on a given case_id.

        :param case_id: The ID of the case to fetch file UUIDs for.
        :return: List of file UUIDs associated with the given case_id.
        """
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
            "fields": "file_id",
            "format": "JSON",
            "size": "1_000_000",
        }
        response = requests.get(self.BASE_URL + self.FILES_ENDPOINT, params=params)
        file_uuids = []
        for file_entry in json.loads(response.content.decode("utf-8"))["data"]["hits"]:
            file_uuids.append(file_entry["file_id"])
        return file_uuids

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def download_files(self, file_uuids, case_id):
        os.makedirs(self.DATA_DIR, exist_ok=True)
        log_file = open(os.path.join(self.DATA_DIR, "gdc.log"), "a")

        params = {"ids": file_uuids}
        response = requests.post(
            self.BASE_URL + self.DATA_ENDPOINT,
            data=json.dumps(params),
            headers={"Content-Type": "application/json"},
        )
        response_head_cd = response.headers["Content-Disposition"]
        if response.status_code == 200:
            os.makedirs(self.DATA_DIR, exist_ok=True)
            file_name = re.findall("filename=(.+)", response_head_cd)[0]
            output_path = os.path.join(self.DATA_DIR, file_name)
            with open(output_path, "wb") as output_file:
                output_file.write(response.content)
            # write to log file
            log_file.write(f"{file_name} {case_id}\n")
        else:
            logging.log("Content-Disposition header missing")

    def download_files_for_case_id(self, case_id):
        """
        Download all files associated with a given case_id.

        :param case_id: The ID of the case to download files for.
        """
        try:
            logging.info(f"Downloading files for case_id: {case_id}")
            file_uuids = self.get_file_uuids_for_case_id(case_id)
            self.download_files(file_uuids, case_id)
        except Exception as e:
            logging.error(f"An error occurred while downloading files: {e}")

    def extract_files(self, ext, mode):
        for filename in os.listdir(self.DATA_DIR):
            if filename.endswith(ext):
                logging.info(f"Extracting {filename}")
                filepath = os.path.join(self.DATA_DIR, filename)
                extraction_successful = False
                while not extraction_successful:
                    try:
                        with tarfile.open(filepath, mode) as tar:
                            tar.extractall(path=self.DATA_DIR)
                            extraction_successful = True
                    except EOFError as e:
                        logging.error(
                            f"EOF Error: {e}. Check gdc.log for case_id details."
                        )
                    except PermissionError as e:
                        logging.error(f"Permission Error: {e}.")
                        continue
                    except Exception as e:
                        logging.error(f"An error occurred while extracting: {e}")

    def organize_files(self, case_id):
        """
        Organize files in the data directory into subdirectories by case_id and data type.

        :param case_id: The ID of the case to organize files for.
        """
        target_dir = os.path.join(self.DATA_DIR, "raw", case_id)
        logging.info(f"Organizing files for case_id: {case_id}")
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

    def post_process_cleanup(self):
        """
        Clean up the data directory by removing all .gz, .tar, .txt, and .log files.
        """
        for filename in os.listdir(self.DATA_DIR):
            if (
                filename.endswith(".gz")
                or filename.endswith(".tar")
                or filename.endswith(".log")
                or filename.endswith(".txt")
            ):
                filepath = os.path.join(self.DATA_DIR, filename)
                os.remove(filepath)

    def multi_download(self, case_ids):
        """
        Concurrently download files for multiple case_ids.

        :param case_ids: List of case IDs to download files for.
        """
        thread_map(
            self.download_files_for_case_id,
            case_ids,
            max_workers=self.MAX_WORKERS,
            progress_bar=False,
        )

    def multi_extract(self):
        """
        Concurrently extract all .gz and .tar files in the data directory.
        """
        thread_map(
            lambda ext, mode: self.extract_files(ext, mode),
            [".gz", ".tar"],
            ["r:gz", "r"],
            max_workers=self.MAX_WORKERS,
            progress_bar=False,
        )

    def multi_organize(self, case_ids):
        """
        Concurrently organize files for multiple case_ids.

        :param case_ids: List of case IDs to organize files for.
        """
        thread_map(
            self.organize_files,
            case_ids,
            max_workers=self.MAX_WORKERS,
            progress_bar=False,
        )

    def process_cases(self, case_ids, case_submitter_ids):
        """
        Process a list of case_ids by downloading, extracting, organizing, and cleaning up files.

        :param case_ids: List of case IDs to process.
        """
        self.multi_download(case_ids)
        self.multi_extract()
        self.multi_organize(case_ids)
        self.post_process_cleanup()


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

        thread_map(
            update_single_entry,
            merged_data,
            max_workers=self.MAX_WORKERS,
        )

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
        all_merged_data = thread_map(
            self.generate_merged_data_for_case,
            case_submitter_ids,
            max_workers=self.MAX_WORKERS,
        )

        all_merged_data = list(chain.from_iterable(all_merged_data))

        self.download_dicom_files(all_merged_data)
        self.update_manifest(all_merged_data)


class TCIAFileDownloader:
    def __init__(self, output_dir, MAX_WORKERS):
        self.output_dir = output_dir
        self.MAX_WORKERS = MAX_WORKERS

    def setApiUrl(self, endpoint, api_url):
        searchEndpoints = [
            "getSeries",
            "getImage",
        ]
        if api_url == "":
            if endpoint in searchEndpoints:
                base_url = (
                    "https://services.cancerimagingarchive.net/nbia-api/services/v1/"
                )
        elif api_url == "nlst":
            if endpoint in searchEndpoints:
                base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/v1/"
        elif api_url == "restricted":
            if endpoint in searchEndpoints:
                if "token_exp_time" in globals() and datetime.now() > token_exp_time:
                    self.refreshToken()
                base_url = (
                    "https://services.cancerimagingarchive.net/nbia-api/services/v2/"
                )
        return base_url

    def refreshToken(self):
        global token_exp_time, api_call_headers, access_token, refresh_token, id_token
        token = refresh_token if "refresh_token" in globals() else ""

        try:
            token_url = "https://keycloak.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"
            params = {
                "client_id": "nbia",
                "grant_type": "refresh_token",
                "refresh_token": token,
            }
            data = requests.post(token_url, data=params)
            data.raise_for_status()
            access_token = data.json()["access_token"]
            expires_in = data.json()["expires_in"]
            current_time = datetime.now()
            token_exp_time = current_time + timedelta(seconds=expires_in)
            api_call_headers = {"Authorization": "Bearer " + access_token}

        except Exception as e:
            print(e)

    def getSeries(
        self,
        collection="",
        patientId="",
        studyUid="",
        seriesUid="",
        modality="",
        bodyPart="",
        manufacturer="",
        manufacturerModel="",
        api_url="",
        format="",
    ):
        endpoint = "getSeries"
        options = {}
        if collection:
            options["Collection"] = collection
        if patientId:
            options["PatientID"] = patientId
        if studyUid:
            options["StudyInstanceUID"] = studyUid
        if seriesUid:
            options["SeriesInstanceUID"] = seriesUid
        if modality:
            options["Modality"] = modality
        if bodyPart:
            options["BodyPartExamined"] = bodyPart
        if manufacturer:
            options["Manufacturer"] = manufacturer
        if manufacturerModel:
            options["ManufacturerModelName"] = manufacturerModel
        base_url = self.setApiUrl(endpoint, api_url)
        url = base_url + endpoint
        try:
            data = requests.get(url, params=options)
            data.raise_for_status()
            if data.text != "":
                data = data.json()
                if format == "df":
                    df = pd.DataFrame(data)
                    return df
                elif format == "csv":
                    df = pd.DataFrame(data)
                    df.to_csv(endpoint + ".csv")
                    return df
                else:
                    return data
            else:
                return None
        except Exception as e:
            print(e)

    def downloadSeries(
        self,
        series_data,
        number=0,
        path="",
        hash="",
        api_url="",
        input_type="",
        format="",
        csv_filename="",
    ):
        endpoint = "getImage"
        seriesUID = ""
        success = 0

        if format == "df" or format == "csv" or csv_filename != "":
            manifestDF = pd.DataFrame()
        base_url = self.setApiUrl(endpoint, api_url)
        if input_type == "df":
            series_data = series_data["SeriesInstanceUID"].tolist()
        if hash == "y":
            downloadOptions = "getImageWithMD5Hash?SeriesInstanceUID="
        else:
            downloadOptions = "getImage?NewFileNames=Yes&SeriesInstanceUID="

        try:
            for x in series_data:
                if input_type == "":
                    seriesUID = x["SeriesInstanceUID"]
                else:
                    seriesUID = x
                if path != "":
                    pathTmp = path + "/" + seriesUID
                else:
                    pathTmp = "tciaDownload/" + seriesUID
                data_url = base_url + downloadOptions + seriesUID
                metadata_url = (
                    base_url + "getSeriesMetaData?SeriesInstanceUID=" + seriesUID
                )
                if not os.path.isdir(pathTmp):
                    if api_url == "restricted":
                        data = requests.get(data_url, headers=api_call_headers)
                    else:
                        data = requests.get(data_url)
                    if data.status_code == 200:
                        if format == "df" or format == "csv" or csv_filename != "":
                            if api_url == "restricted":
                                metadata = requests.get(
                                    metadata_url, headers=api_call_headers
                                ).json()
                            else:
                                metadata = requests.get(metadata_url).json()
                            manifestDF = pd.concat(
                                [manifestDF, pd.DataFrame(metadata)], ignore_index=True
                            )
                        file = zipfile.ZipFile(io.BytesIO(data.content))
                        file.extractall(path=pathTmp)
                        success += 1
                        if number > 0:
                            if success == number:
                                break
                else:
                    if format == "df" or format == "csv" or csv_filename != "":
                        if api_url == "restricted":
                            metadata = requests.get(
                                metadata_url, headers=api_call_headers
                            ).json()
                        else:
                            metadata = requests.get(metadata_url).json()
                        manifestDF = pd.concat(
                            [manifestDF, pd.DataFrame(metadata)], ignore_index=True
                        )
            if csv_filename != "":
                manifestDF.to_csv(csv_filename + ".csv")
                return manifestDF
            if format == "csv" and csv_filename == "":
                now = datetime.now()
                dt_string = now.strftime("%Y-%m-%d_%H%M")
                manifestDF.to_csv("downloadSeries_metadata_" + dt_string + ".csv")
                return manifestDF
            if format == "df":
                return manifestDF
        except Exception as e:
            print(e)

    def manage_files(self, manifest):
        # generate folders for each case_submitter_id skip if already exists
        for case_submitter_id in tqdm(manifest["PatientID"].unique()):
            case_submitter_id_path = os.path.join(
                self.output_dir, "raw", case_submitter_id
            )
            if os.path.exists(case_submitter_id_path):
                continue
            else:
                os.makedirs(case_submitter_id_path)

        # move all SeriesInstanceUID folders to their respective case_submitter_id folders
        for case_submitter_id in tqdm(manifest["PatientID"].unique()):
            case_submitter_id_path = os.path.join(
                self.output_dir, "raw", case_submitter_id
            )
            for series_instance_uid in manifest[
                manifest["PatientID"] == case_submitter_id
            ]["SeriesInstanceUID"].unique():
                series_instance_uid_path = os.path.join(
                    self.output_dir, series_instance_uid
                )

                modality = manifest[
                    (manifest["PatientID"] == case_submitter_id)
                    & (manifest["SeriesInstanceUID"] == series_instance_uid)
                ]["Modality"].values[0]
                modality_path = os.path.join(case_submitter_id_path, modality)
                os.makedirs(modality_path, exist_ok=True)
                shutil.move(series_instance_uid_path, modality_path)

    def process_cases(self, case_submitter_ids):
        results = []
        for case_submitter_id in tqdm(case_submitter_ids):
            series = self.getSeries(patientId=case_submitter_id[0], format="df")
            if series is None:
                continue
            else:
                self.downloadSeries(
                    series_data=series, path=self.output_dir, input_type="df"
                )
                results.append(series)

        if results:
            final_df = pd.concat(results)
        else:
            final_df = pd.DataFrame()
        if final_df.empty:
            return

        final_df.to_csv(
            os.path.join(self.output_dir, "radiology_metadata.csv"), index=False
        )

        # load manifest
        manifest = pd.read_csv(os.path.join(self.output_dir, "radiology_metadata.csv"))

        # manage files
        self.manage_files(manifest)
