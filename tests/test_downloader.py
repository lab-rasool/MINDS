"""
Tests for the downloader classes (GDCFileDownloader, IDCFileDownloader, TCIAFileDownloader).
"""
import os
import pytest
import responses
from unittest.mock import MagicMock, patch, mock_open
from app.med_minds.downloader import (
    GDCFileDownloader,
    IDCFileDownloader,
    TCIAFileDownloader
)


class TestGDCFileDownloader:
    """Tests for GDCFileDownloader class."""

    def test_init(self, temp_dir, sample_manifest_file):
        """Test GDCFileDownloader initialization."""
        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)

        assert downloader.BASE_URL == "https://api.gdc.cancer.gov/"
        assert downloader.DATA_ENDPOINT == "data"
        assert downloader.DATA_DIR == temp_dir
        assert downloader.MAX_WORKERS == 4
        assert len(downloader.manifest) > 0

    def test_init_with_filters(self, temp_dir, sample_manifest_file):
        """Test initialization with include/exclude filters."""
        downloader = GDCFileDownloader(
            temp_dir,
            MAX_WORKERS=4,
            include=["CT", "MR"],
            exclude=["Slide Image"]
        )

        assert downloader.include == ["CT", "MR"]
        assert downloader.exclude == ["Slide Image"]

    @responses.activate
    def test_download_files_success(self, temp_dir, sample_manifest_file):
        """Test successful file download."""
        responses.add(
            responses.POST,
            "https://api.gdc.cancer.gov/data",
            body=b"test file content",
            status=200,
            headers={'Content-Disposition': 'filename=test.bam'}
        )

        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)

        with patch('builtins.open', mock_open()) as mock_file:
            downloader.download_files('file-uuid-1', 'case-1')
            # Verify file was written
            assert mock_file.called

    @responses.activate
    def test_download_files_retry(self, temp_dir, sample_manifest_file):
        """Test download with missing header (doesn't retry without exception)."""
        # Add response without Content-Disposition header
        responses.add(
            responses.POST,
            "https://api.gdc.cancer.gov/data",
            body=b"test content",
            status=200
            # No Content-Disposition header
        )

        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)
        downloader.download_files('file-uuid-1', 'case-1')

        # Should have been called once (no retry without exception)
        assert len(responses.calls) == 1

    def test_download_files_for_patient(self, temp_dir, sample_manifest_file):
        """Test downloading files for a specific patient."""
        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)

        patient_data = {
            "PatientID": "TCGA-01",
            "gdc_case_id": "case-1",
            "Aligned Reads": [
                {"id": "file-uuid-1", "file_name": "test.bam"}
            ]
        }

        with patch.object(downloader, 'download_files') as mock_download:
            downloader.download_files_for_patient(patient_data)
            mock_download.assert_called_once()

    def test_download_files_for_patient_with_include(self, temp_dir, sample_manifest_file):
        """Test downloading with include filter."""
        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4, include=["Aligned Reads"])

        patient_data = {
            "PatientID": "TCGA-01",
            "gdc_case_id": "case-1",
            "Aligned Reads": [{"id": "file-1"}],
            "Clinical Supplement": [{"id": "file-2"}]
        }

        with patch.object(downloader, 'download_files') as mock_download:
            downloader.download_files_for_patient(patient_data)
            # Should only download Aligned Reads
            assert mock_download.call_count == 1

    def test_download_files_for_patient_with_exclude(self, temp_dir, sample_manifest_file):
        """Test downloading with exclude filter."""
        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4, exclude=["Slide Image"])

        patient_data = {
            "PatientID": "TCGA-01",
            "gdc_case_id": "case-1",
            "Aligned Reads": [{"id": "file-1"}],
            "Slide Image": [{"id": "file-2"}]
        }

        with patch.object(downloader, 'download_files') as mock_download:
            downloader.download_files_for_patient(patient_data)
            # Should not download Slide Image
            assert mock_download.call_count == 1

    # @patch('app.med_minds.downloader.ThreadPoolExecutor')
    # @patch('app.med_minds.downloader.Progress')
    # def test_multi_download(self, mock_progress_cls, mock_executor_cls, temp_dir, sample_manifest_file):
    #     """Test multi-threaded download."""
    #     mock_progress = MagicMock()
    #     mock_progress_cls.return_value.__enter__.return_value = mock_progress
    #     mock_progress_cls.return_value.__exit__.return_value = None

    #     mock_executor = MagicMock()
    #     mock_executor_cls.return_value.__enter__.return_value = mock_executor

    #     downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)

    #     with patch.object(downloader, 'download_files_for_patient'):
    #         downloader.multi_download()

    #     # Verify tasks were submitted
    #     assert mock_executor.submit.called

    @patch('app.med_minds.downloader.ThreadPoolExecutor')
    def test_multi_extract(self, mock_executor_cls, temp_dir, sample_manifest_file):
        """Test multi-threaded extraction."""
        mock_executor = MagicMock()
        mock_executor_cls.return_value.__enter__.return_value = mock_executor
        mock_executor_cls.return_value.__exit__.return_value = None

        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)
        downloader.multi_extract()

        # Verify extract tasks were submitted
        assert mock_executor.submit.call_count == 2  # For .tar.gz and .tar

    def test_process_cases(self, temp_dir, sample_manifest_file):
        """Test full processing workflow."""
        downloader = GDCFileDownloader(temp_dir, MAX_WORKERS=4)

        with patch.object(downloader, 'multi_download') as mock_download, \
             patch.object(downloader, 'multi_extract') as mock_extract, \
             patch.object(downloader, 'organize_files') as mock_organize, \
             patch.object(downloader, 'post_process_cleanup') as mock_cleanup:

            downloader.process_cases()

            mock_download.assert_called_once()
            mock_extract.assert_called_once()
            mock_organize.assert_called_once()
            mock_cleanup.assert_called_once()


class TestTCIAFileDownloader:
    """Tests for TCIAFileDownloader class (deprecated)."""

    def test_init_shows_deprecation_warning(self, temp_dir, sample_manifest_file):
        """Test that initialization shows deprecation warning."""
        with pytest.warns(DeprecationWarning):
            downloader = TCIAFileDownloader(temp_dir, MAX_WORKERS=4)

    def test_init_with_filters(self, temp_dir, sample_manifest_file):
        """Test initialization with filters."""
        with pytest.warns(DeprecationWarning):
            downloader = TCIAFileDownloader(
                temp_dir,
                MAX_WORKERS=4,
                include=["CT", "MR"]
            )
            assert downloader.include == ["CT", "MR"]

    @responses.activate
    def test_download_helper_success(self, temp_dir, sample_manifest_file):
        """Test successful DICOM series download."""
        with pytest.warns(DeprecationWarning):
            downloader = TCIAFileDownloader(temp_dir, MAX_WORKERS=4)

        # Create a minimal valid ZIP file
        import io
        import zipfile
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr('test.dcm', b'test dicom content')
        zip_content = zip_buffer.getvalue()

        responses.add(
            responses.GET,
            "https://test.url/download",
            body=zip_content,
            status=200,
            content_type='application/zip'
        )

        mock_progress = MagicMock()
        mock_task = MagicMock()

        with patch('app.med_minds.downloader.os.path.isdir', return_value=False):
            downloader.download_helper("https://test.url/download", temp_dir, mock_progress, mock_task)
            # Verify the progress was updated
            mock_progress.update.assert_called_once()


class TestIDCFileDownloader:
    """Tests for IDCFileDownloader class."""

    def test_init(self, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test IDCFileDownloader initialization."""
        downloader = IDCFileDownloader(temp_dir, MAX_WORKERS=4)

        assert downloader.save_directory == temp_dir
        assert downloader.MAX_WORKERS == 4
        assert downloader.idc_api_preamble == "https://api.imaging.datacommons.cancer.gov/v2"

    @responses.activate
    def test_make_api_call_success(self, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test successful API call to IDC."""
        test_url = "https://api.imaging.datacommons.cancer.gov/v2/test"
        responses.add(
            responses.POST,
            test_url,
            json={"status": "success"},
            status=200
        )

        downloader = IDCFileDownloader(temp_dir, MAX_WORKERS=4)

        result = downloader.make_api_call(test_url, {}, {})
        assert result["status"] == "success"

    @responses.activate
    def test_make_api_call_retry(self, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test API call retry on failure."""
        test_url = "https://api.imaging.datacommons.cancer.gov/v2/test"
        # First call fails, second succeeds
        responses.add(
            responses.POST,
            test_url,
            status=500
        )
        responses.add(
            responses.POST,
            test_url,
            json={"status": "success"},
            status=200
        )

        downloader = IDCFileDownloader(temp_dir, MAX_WORKERS=4)

        result = downloader.make_api_call(test_url, {}, {})
        assert result["status"] == "success"
        assert len(responses.calls) == 2

    def test_init_with_filters(self, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test initialization with modality filters."""
        downloader = IDCFileDownloader(
            temp_dir,
            MAX_WORKERS=4,
            include=["CT", "MR"]
        )

        assert downloader.include == ["CT", "MR"]

    @patch('app.med_minds.downloader.storage.Client.create_anonymous_client')
    def test_download_dicom_files(self, mock_create_client, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test DICOM file download from GCS."""
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_create_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        downloader = IDCFileDownloader(temp_dir, MAX_WORKERS=4)

        merged_data = [{
            "SeriesInstanceUID": "series-1",
            "GCS_URL": "gs://bucket/path/file.dcm",
            "Modality": "CT",
            "Patient_ID": "TCGA-01"
        }]

        with patch('app.med_minds.downloader.os.makedirs'):
            downloader.download_dicom_files(merged_data)

        # Verify storage client was created
        mock_create_client.assert_called()

    def test_update_manifest(self, temp_dir, sample_manifest_file, sample_cohort_data):
        """Test manifest update with IDC data."""
        downloader = IDCFileDownloader(temp_dir, MAX_WORKERS=4)

        merged_data = [{
            "SeriesInstanceUID": "series-1",
            "GCS_URL": "gs://bucket/path/to/series-1/file.dcm",
            "Modality": "CT",
            "Patient_ID": "TCGA-01",
            "Collection_ID": "TCGA-LUAD"
        }]

        # The update_manifest method takes only merged_data, not patient_id
        downloader.update_manifest(merged_data)

        # Verify manifest was updated by checking the file exists and is valid
        manifest_path = os.path.join(temp_dir, "manifest.json")
        assert os.path.exists(manifest_path)
