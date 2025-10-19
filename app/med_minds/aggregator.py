import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
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
    elif isinstance(data, np.ndarray):
        return numpy_to_python(data.tolist())
    elif isinstance(data, (np.integer, np.floating)):
        return data.item()
    elif isinstance(data, (np.str_, np.bytes_)):
        return str(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    else:
        return data


class Aggregator:
    """
    Aggregator class for managing and processing cohort data from GDC and TCIA sources.
    Attributes:
        cohort (pd.DataFrame): A DataFrame containing cohort information with case IDs and submitter IDs.
        case_ids (list): A list of case IDs extracted from the cohort.
        case_submitter_ids (list): A list of case submitter IDs extracted from the cohort.
        DATA_DIR (str): Directory path where output files, such as the manifest, will be stored.
        MAX_WORKERS (int): Maximum number of workers for concurrent processing.
        GDC_BASE_URL (str): Base URL for the GDC API.
        TCIA_BASE_URL (str): Base URL for the TCIA API.
        manifest_path (str): Path to the manifest JSON file.
        structured_manifest_all_modalities (list): A list to store structured data for all modalities.
    Methods:
        gdc_files(case_id, case_submitter_id):
            Fetches file information from the GDC API for a given case ID and organizes it by data type.
        tcia_files(patient_id):
            Fetches file information from the TCIA API for a given patient ID.
        add_or_update_entry_all_modalities(patient_id, modality_data, modality):
            Adds or updates an entry in the structured manifest for a specific patient and modality.
        format_tcia_response(tcia_response):
            Processes the TCIA response and organizes data by modality.
        generate_manifest():
            Generates a manifest by aggregating data from GDC and TCIA sources, and writes it to a JSON file.
    """

    def __init__(self, cohort, output_dir, max_workers=8):
        self.cohort = cohort
        self.case_ids = self.cohort.index.to_list()
        self.case_submitter_ids = self.cohort.values.tolist()
        self.DATA_DIR = output_dir
        self.MAX_WORKERS = max_workers
        self.GDC_BASE_URL = "https://api.gdc.cancer.gov/"
        self.TCIA_BASE_URL = (
            "https://services.cancerimagingarchive.net/services/v4/TCIA/query/"
        )
        self.IDC_BASE_URL = "https://api.imaging.datacommons.cancer.gov/v2"
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
        """
        Fetch imaging series from TCIA API for a given patient ID.

        DEPRECATED: TCIA is no longer hosting controlled-access data due to NIH policy changes.
        Use idc_files() instead to access data from the Imaging Data Commons.
        """
        import warnings

        warnings.warn(
            "TCIA API is deprecated. Use IDC API instead via idc_files() method.",
            DeprecationWarning,
            stacklevel=2,
        )

        output_format = "&format=json"
        GET_SERIES_BY_PATIENT_ID = "getSeries?PatientID="
        # Handle patient_id as either a list or string
        patient_id_str = patient_id[0] if isinstance(patient_id, list) else patient_id
        url = self.TCIA_BASE_URL + GET_SERIES_BY_PATIENT_ID + patient_id_str + output_format
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Error in TCIA request: {e}")
            return None

    @retry(tries=5, delay=5, backoff=2, jitter=(2, 9))
    def idc_files(self, patient_id):
        """Fetch imaging series from IDC v2 API for a given patient ID."""
        url = f"{self.IDC_BASE_URL}/cohorts/manifest/preview"

        # Convert patient_id to native Python type to ensure JSON serialization works
        patient_id_clean = numpy_to_python(patient_id)

        # v2 API uses different filter structure
        # Determine collection from patient ID (e.g., TCGA-XX-XXXX -> tcga_luad for TCGA-LUAD patients)
        pid = patient_id_clean[0] if isinstance(patient_id_clean, list) else patient_id_clean
        collection_id = None
        if pid.startswith("TCGA-"):
            # For TCGA data, we'll let IDC search across all TCGA collections
            # Alternatively, we could infer the collection from the project_id if we had that info
            pass

        filters = {"PatientID": [pid]}
        # Note: Without knowing the specific collection, we query by PatientID only
        # IDC will search across all collections

        # IDC v2 API requires fields (lowercase field names for most, CamelCase for DICOM tags)
        fields = [
            "collection_id",
            "PatientID",
            "Modality",  # Add Modality directly to manifest
            "StudyInstanceUID",
            "SeriesInstanceUID",
            "SOPInstanceUID",
            "gcs_url",
            "crdc_series_uuid"
        ]

        body = {
            "cohort_def": {
                "name": "MINDS_temp",
                "description": "Temporary cohort for patient data",
                "filters": filters,
            },
            "fields": fields
        }

        params = {
            "sql": False
        }

        try:
            response = requests.post(url, params=params, json=body)
            if response.status_code == 200:
                data = response.json()
                # IDC v2 API uses 'manifest_data' key (not 'json_manifest')
                manifest_data = data.get('manifest', {}).get('manifest_data', [])
                # Also fetch metadata for modality information
                query_url = f"{self.IDC_BASE_URL}/cohorts/query/preview"
                query_body = {
                    "cohort_def": {
                        "name": "MINDS_temp",
                        "description": "Temporary cohort",
                        "filters": filters,
                    },
                    "queryFields": {
                        "fields": [
                            "PatientID",
                            "SeriesInstanceUID",
                            "Modality",
                            "collection_id",
                            "StudyDescription",
                            "SeriesDescription",
                        ]
                    },
                }
                query_params = {"sql": False}
                query_response = requests.post(query_url, params=query_params, json=query_body)

                if query_response.status_code == 200:
                    query_data = query_response.json()
                    return {"manifest": data, "metadata": query_data}
                else:
                    return {"manifest": data, "metadata": None}
            return None
        except Exception as e:
            print(f"Error in IDC request for {patient_id_clean}: {e}")
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

    def format_idc_response(self, idc_response):
        """Format IDC v2 API response and organize data by modality."""
        idc_modalities = [
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

        for patient_data in idc_response:
            if not patient_data or not isinstance(patient_data, dict):
                continue

            manifest_data = patient_data.get("manifest", {})
            metadata = patient_data.get("metadata", {})

            # Extract manifest entries (IDC v2 uses 'manifest_data' key)
            manifest_entries = manifest_data.get("manifest", {}).get("manifest_data", [])

            # Process each manifest entry
            # IDC v2 API includes Modality directly in manifest_data
            for item in manifest_entries:
                series_uid = item.get("SeriesInstanceUID")
                patient_id = item.get("PatientID")  # IDC v2 uses 'PatientID' not 'Patient_ID'
                modality = item.get("Modality")

                if not series_uid or not patient_id:
                    continue

                if not modality:
                    continue

                if modality in idc_modalities:
                    modality_data = {
                        "SeriesInstanceUID": series_uid,
                        "gcs_url": item.get("gcs_url"),  # IDC v2 uses lowercase 'gcs_url'
                        "collection_id": item.get("collection_id"),  # IDC v2 uses lowercase 'collection_id'
                        "crdc_series_uuid": item.get("crdc_series_uuid"),  # IDC v2 uses this instead of CRDC_Series_GUID
                        "StudyInstanceUID": item.get("StudyInstanceUID"),
                        "SOPInstanceUID": item.get("SOPInstanceUID"),
                        "source": "IDC",
                    }
                    self.add_or_update_entry_all_modalities(
                        patient_id, modality_data, modality
                    )

    def generate_manifest(self, use_idc=True):
        """
        Generate manifest by aggregating data from GDC and imaging sources.

        Args:
            use_idc (bool): If True, use IDC API (default). If False, fall back to TCIA.
        """
        # Step 1: Fetch GDC data
        with Progress() as progress:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                task = progress.add_task(
                    "Generating manifest from GDC...", total=len(self.case_ids)
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

        # Step 2: Fetch imaging data from IDC or TCIA
        if use_idc:
            # Use IDC v2 API
            with Progress() as progress:
                with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                    task = progress.add_task(
                        "Aggregating imaging data from IDC...",
                        total=len(self.case_submitter_ids),
                    )
                    futures = {
                        executor.submit(self.idc_files, patient_id): patient_id
                        for patient_id in self.case_submitter_ids
                    }

                    all_idc_responses = []
                    for future in as_completed(futures):
                        progress.update(task, advance=1)
                        idc_response = future.result()
                        all_idc_responses.append(idc_response)

            self.format_idc_response(all_idc_responses)
        else:
            # Fall back to TCIA (deprecated)
            import warnings

            warnings.warn(
                "TCIA API is deprecated due to NIH policy changes. "
                "MINDS now uses the Imaging Data Commons (IDC) API by default. "
                "TCIA support will be removed in a future version. "
                "See: https://www.cancerimagingarchive.net/new-nih-policies-for-controlled-access-data/",
                DeprecationWarning,
                stacklevel=2,
            )

            with Progress() as progress:
                with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                    task = progress.add_task(
                        "Aggregating imaging data from TCIA (deprecated)...",
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

        # Step 3: Merge imaging data with GDC manifest
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
