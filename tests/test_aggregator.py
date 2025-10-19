"""
Tests for the Aggregator class.
"""
import json
import pytest
import responses
from unittest.mock import Mock, MagicMock, patch, mock_open
from app.med_minds.aggregator import Aggregator, numpy_to_python


class TestNumpyToPython:
    """Tests for numpy_to_python helper function."""

    def test_convert_dict(self):
        """Test converting dictionaries."""
        data = {'key1': 'value1', 'key2': 'value2'}
        result = numpy_to_python(data)
        assert result == data

    def test_convert_list(self):
        """Test converting lists."""
        data = [1, 2, 3, 4]
        result = numpy_to_python(data)
        assert result == data

    def test_convert_nested(self):
        """Test converting nested structures."""
        data = {'list': [1, 2, {'nested': 'value'}], 'key': 'value'}
        result = numpy_to_python(data)
        assert result == data


class TestAggregator:
    """Tests for Aggregator class."""

    def test_init(self, sample_cohort_data, temp_dir):
        """Test Aggregator initialization."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)

        assert aggregator.cohort.equals(sample_cohort_data)
        assert aggregator.case_ids == ['case-1', 'case-2', 'case-3']
        assert aggregator.DATA_DIR == temp_dir
        assert aggregator.MAX_WORKERS == 8
        assert aggregator.GDC_BASE_URL == "https://api.gdc.cancer.gov/"
        assert aggregator.IDC_BASE_URL == "https://api.imaging.datacommons.cancer.gov/v2"

    @responses.activate
    def test_gdc_files_success(self, sample_cohort_data, temp_dir, sample_gdc_response):
        """Test fetching GDC files successfully."""
        responses.add(
            responses.GET,
            "https://api.gdc.cancer.gov/files",
            json=sample_gdc_response,
            status=200
        )

        aggregator = Aggregator(sample_cohort_data, temp_dir)
        result = aggregator.gdc_files('case-1', ['TCGA-01'])

        assert result['PatientID'] == 'TCGA-01'
        assert result['gdc_case_id'] == 'case-1'
        assert 'Aligned Reads' in result
        assert 'Clinical Supplement' in result
        assert len(result['Aligned Reads']) == 1
        assert result['Aligned Reads'][0]['file_name'] == 'sample1.bam'

    @responses.activate
    def test_idc_files_success(self, sample_cohort_data, temp_dir):
        """Test fetching IDC files successfully."""
        manifest_response = {
            "manifest": {
                "json_manifest": [
                    {
                        "SeriesInstanceUID": "series-1",
                        "Patient_ID": "TCGA-01",
                        "GCS_URL": "gs://bucket/file1.dcm",
                        "Collection_ID": "TCGA-LUAD",
                        "CRDC_Series_GUID": "guid-1"
                    }
                ]
            }
        }

        metadata_response = {
            "query_results": {
                "json": [
                    {
                        "SeriesInstanceUID": "series-1",
                        "PatientID": "TCGA-01",
                        "Modality": "CT",
                        "StudyDescription": "Test Study",
                        "SeriesDescription": "Test Series"
                    }
                ]
            }
        }

        # Mock manifest endpoint
        responses.add(
            responses.POST,
            "https://api.imaging.datacommons.cancer.gov/v2/cohorts/manifest/preview",
            json=manifest_response,
            status=200
        )

        # Mock query endpoint
        responses.add(
            responses.POST,
            "https://api.imaging.datacommons.cancer.gov/v2/cohorts/query/preview",
            json=metadata_response,
            status=200
        )

        aggregator = Aggregator(sample_cohort_data, temp_dir)
        result = aggregator.idc_files(['TCGA-01'])

        assert result is not None
        assert 'manifest' in result
        assert 'metadata' in result

    @responses.activate
    def test_idc_files_failure(self, sample_cohort_data, temp_dir):
        """Test IDC API failure handling."""
        responses.add(
            responses.POST,
            "https://api.imaging.datacommons.cancer.gov/v2/cohorts/manifest/preview",
            status=500
        )

        aggregator = Aggregator(sample_cohort_data, temp_dir)
        result = aggregator.idc_files(['TCGA-01'])

        assert result is None

    def test_add_or_update_entry_new_patient(self, sample_cohort_data, temp_dir):
        """Test adding entry for new patient."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)
        aggregator.structured_manifest_all_modalities = []

        modality_data = {"file": "test.dcm"}
        aggregator.add_or_update_entry_all_modalities("TCGA-01", modality_data, "CT")

        assert len(aggregator.structured_manifest_all_modalities) == 1
        assert aggregator.structured_manifest_all_modalities[0]['PatientID'] == 'TCGA-01'
        assert 'CT' in aggregator.structured_manifest_all_modalities[0]

    def test_add_or_update_entry_existing_patient(self, sample_cohort_data, temp_dir):
        """Test updating entry for existing patient."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)
        aggregator.structured_manifest_all_modalities = [
            {"PatientID": "TCGA-01", "CT": [{"file": "existing.dcm"}]}
        ]

        modality_data = {"file": "new.dcm"}
        aggregator.add_or_update_entry_all_modalities("TCGA-01", modality_data, "CT")

        assert len(aggregator.structured_manifest_all_modalities) == 1
        assert len(aggregator.structured_manifest_all_modalities[0]['CT']) == 2

    def test_add_or_update_entry_new_modality(self, sample_cohort_data, temp_dir):
        """Test adding new modality for existing patient."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)
        aggregator.structured_manifest_all_modalities = [
            {"PatientID": "TCGA-01", "CT": [{"file": "ct.dcm"}]}
        ]

        modality_data = {"file": "mr.dcm"}
        aggregator.add_or_update_entry_all_modalities("TCGA-01", modality_data, "MR")

        assert len(aggregator.structured_manifest_all_modalities) == 1
        assert 'CT' in aggregator.structured_manifest_all_modalities[0]
        assert 'MR' in aggregator.structured_manifest_all_modalities[0]

    def test_format_idc_response(self, sample_cohort_data, temp_dir):
        """Test formatting IDC response."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)

        idc_response = [
            {
                "manifest": {
                    "manifest": {
                        "json_manifest": [
                            {
                                "SeriesInstanceUID": "series-1",
                                "Patient_ID": "TCGA-01",
                                "GCS_URL": "gs://bucket/file.dcm",
                                "Collection_ID": "TCGA-LUAD",
                                "CRDC_Series_GUID": "guid-1"
                            }
                        ]
                    }
                },
                "metadata": {
                    "query_results": {
                        "json": [
                            {
                                "SeriesInstanceUID": "series-1",
                                "Modality": "CT",
                                "StudyDescription": "Study",
                                "SeriesDescription": "Series"
                            }
                        ]
                    }
                }
            }
        ]

        aggregator.format_idc_response(idc_response)

        assert len(aggregator.structured_manifest_all_modalities) == 1
        assert aggregator.structured_manifest_all_modalities[0]['PatientID'] == 'TCGA-01'
        assert 'CT' in aggregator.structured_manifest_all_modalities[0]

    @patch('app.med_minds.aggregator.ThreadPoolExecutor')
    @patch('app.med_minds.aggregator.Progress')
    def test_generate_manifest_with_idc(self, mock_progress_cls, mock_executor_cls,
                                        sample_cohort_data, temp_dir):
        """Test manifest generation with IDC."""
        # Mock Progress
        mock_progress = MagicMock()
        mock_progress_cls.return_value.__enter__.return_value = mock_progress
        mock_progress_cls.return_value.__exit__.return_value = None

        # Mock ThreadPoolExecutor
        mock_executor = MagicMock()
        mock_executor_cls.return_value.__enter__.return_value = mock_executor

        # Mock futures for GDC
        mock_gdc_future = MagicMock()
        mock_gdc_future.result.return_value = {
            "PatientID": "TCGA-01",
            "gdc_case_id": "case-1",
            "Clinical Supplement": [{"file_id": "file-1"}]
        }

        # Mock futures for IDC
        mock_idc_future = MagicMock()
        mock_idc_future.result.return_value = None

        mock_executor.submit.side_effect = [mock_gdc_future, mock_gdc_future, mock_gdc_future,
                                             mock_idc_future, mock_idc_future, mock_idc_future]

        # Mock as_completed
        with patch('app.med_minds.aggregator.as_completed') as mock_as_completed, \
             patch('builtins.open', mock_open(read_data='[]')) as mock_file, \
             patch('json.dump') as mock_json_dump:
            mock_as_completed.side_effect = [
                [mock_gdc_future, mock_gdc_future, mock_gdc_future],
                [mock_idc_future, mock_idc_future, mock_idc_future]
            ]

            aggregator = Aggregator(sample_cohort_data, temp_dir)
            aggregator.generate_manifest(use_idc=True)

            # Verify JSON dump was called (manifest was written)
            assert mock_json_dump.call_count >= 2

    @responses.activate
    def test_tcia_files_deprecated_warning(self, sample_cohort_data, temp_dir):
        """Test that TCIA files method shows deprecation warning."""
        responses.add(
            responses.GET,
            "https://services.cancerimagingarchive.net/services/v4/TCIA/query/getSeries",
            json=[],
            status=200
        )

        aggregator = Aggregator(sample_cohort_data, temp_dir)

        with pytest.warns(DeprecationWarning):
            result = aggregator.tcia_files(["TCGA-01"])

    def test_format_tcia_response(self, sample_cohort_data, temp_dir):
        """Test formatting TCIA response."""
        aggregator = Aggregator(sample_cohort_data, temp_dir)

        tcia_response = [
            [
                {
                    "PatientID": "TCGA-01",
                    "Modality": "CT",
                    "SeriesInstanceUID": "series-1",
                    "StudyInstanceUID": "study-1"
                }
            ]
        ]

        aggregator.format_tcia_response(tcia_response)

        assert len(aggregator.structured_manifest_all_modalities) == 1
        assert 'CT' in aggregator.structured_manifest_all_modalities[0]

    @responses.activate
    def test_gdc_files_retry_on_failure(self, sample_cohort_data, temp_dir, sample_gdc_response):
        """Test that GDC files retries on failure."""
        # First call fails, subsequent calls succeed
        responses.add(
            responses.GET,
            "https://api.gdc.cancer.gov/files",
            status=500
        )
        responses.add(
            responses.GET,
            "https://api.gdc.cancer.gov/files",
            json=sample_gdc_response,
            status=200
        )

        aggregator = Aggregator(sample_cohort_data, temp_dir)
        result = aggregator.gdc_files('case-1', ['TCGA-01'])

        # Should succeed after retry
        assert result['PatientID'] == 'TCGA-01'
        assert len(responses.calls) == 2
