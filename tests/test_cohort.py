"""
Tests for the Cohort class.
"""
import json
import os
import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from app.med_minds import Cohort


class TestCohort:
    """Tests for Cohort class."""

    def test_init(self, sample_cohort_data, temp_dir):
        """Test Cohort initialization."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        assert cohort.data.equals(sample_cohort_data)
        assert cohort.output_dir == temp_dir
        assert cohort.manifest_file == os.path.join(temp_dir, "manifest.json")

    def test_init_creates_output_dir(self, sample_cohort_data):
        """Test that Cohort creates output directory if it doesn't exist."""
        with patch('app.med_minds.os.makedirs') as mock_makedirs, \
             patch('app.med_minds.os.path.exists') as mock_exists:
            mock_exists.return_value = False

            cohort = Cohort(sample_cohort_data, "/nonexistent/dir")

            mock_makedirs.assert_called_once_with("/nonexistent/dir")

    def test_generate_manifest(self, sample_cohort_data, temp_dir):
        """Test manifest generation."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.Aggregator') as mock_aggregator_cls:
            mock_aggregator = MagicMock()
            mock_aggregator_cls.return_value = mock_aggregator

            cohort.generate_manifest()

            mock_aggregator_cls.assert_called_once_with(sample_cohort_data, temp_dir)
            mock_aggregator.generate_manifest.assert_called_once()

    def test_download_without_manifest_raises_error(self, sample_cohort_data, temp_dir):
        """Test that download raises error if manifest doesn't exist."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.os.path.exists') as mock_exists:
            mock_exists.return_value = False

            with pytest.raises(FileNotFoundError):
                cohort.download()

    def test_download_with_idc(self, sample_cohort_data, temp_dir, sample_manifest_file):
        """Test download using IDC (default)."""
        cohort = Cohort(sample_cohort_data, temp_dir)
        cohort.manifest_file = sample_manifest_file

        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files') as mock_gdc, \
             patch.object(cohort, '_download_idc_files') as mock_idc:
            mock_exists.return_value = True

            cohort.download(threads=4, use_idc=True)

            mock_gdc.assert_called_once_with(4, include=None, exclude=None)
            mock_idc.assert_called_once_with(4, include=None, exclude=None)

    def test_download_with_tcia_shows_warning(self, sample_cohort_data, temp_dir, sample_manifest_file):
        """Test that using TCIA shows deprecation warning."""
        cohort = Cohort(sample_cohort_data, temp_dir)
        cohort.manifest_file = sample_manifest_file

        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files'), \
             patch.object(cohort, '_download_tcia_files'), \
             pytest.warns(DeprecationWarning):
            mock_exists.return_value = True

            cohort.download(threads=4, use_idc=False)

    def test_download_with_include_filter(self, sample_cohort_data, temp_dir, sample_manifest_file):
        """Test download with include filter."""
        cohort = Cohort(sample_cohort_data, temp_dir)
        cohort.manifest_file = sample_manifest_file

        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files') as mock_gdc, \
             patch.object(cohort, '_download_idc_files') as mock_idc:
            mock_exists.return_value = True

            cohort.download(threads=4, include=["CT", "MR"])

            mock_gdc.assert_called_once_with(4, include=["CT", "MR"], exclude=None)
            mock_idc.assert_called_once_with(4, include=["CT", "MR"], exclude=None)

    def test_download_with_exclude_filter(self, sample_cohort_data, temp_dir, sample_manifest_file):
        """Test download with exclude filter."""
        cohort = Cohort(sample_cohort_data, temp_dir)
        cohort.manifest_file = sample_manifest_file

        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files') as mock_gdc, \
             patch.object(cohort, '_download_idc_files') as mock_idc:
            mock_exists.return_value = True

            cohort.download(threads=4, exclude=["Slide Image"])

            mock_gdc.assert_called_once_with(4, include=None, exclude=["Slide Image"])
            mock_idc.assert_called_once_with(4, include=None, exclude=["Slide Image"])

    def test_stats(self, sample_cohort_data, temp_dir, sample_manifest):
        """Test cohort statistics calculation."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('builtins.open', mock_open(read_data=json.dumps(sample_manifest))):
            stats = cohort.stats()

            assert isinstance(stats, dict)
            assert "Aligned Reads" in stats
            assert "file_count" in stats["Aligned Reads"]
            assert "total_size" in stats["Aligned Reads"]

    def test_stats_calculates_sizes(self, sample_cohort_data, temp_dir):
        """Test that stats correctly calculates file sizes."""
        manifest = [
            {
                "PatientID": "TCGA-01",
                "Clinical Supplement": [
                    {"file_size": 1024},
                    {"file_size": 2048}
                ]
            }
        ]

        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('builtins.open', mock_open(read_data=json.dumps(manifest))):
            stats = cohort.stats()

            assert stats["Clinical Supplement"]["file_count"] == 2
            assert stats["Clinical Supplement"]["total_size"] == 3072

    def test_download_gdc_files(self, sample_cohort_data, temp_dir):
        """Test GDC file download method."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.GDCFileDownloader') as mock_downloader_cls:
            mock_downloader = MagicMock()
            mock_downloader_cls.return_value = mock_downloader

            cohort._download_gdc_files(threads=4, include=["CT"], exclude=["MR"])

            mock_downloader_cls.assert_called_once_with(
                temp_dir,
                MAX_WORKERS=4,
                include=["CT"],
                exclude=["MR"]
            )
            mock_downloader.process_cases.assert_called_once()

    def test_download_idc_files(self, sample_cohort_data, temp_dir):
        """Test IDC file download method."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.IDCFileDownloader') as mock_downloader_cls:
            mock_downloader = MagicMock()
            mock_downloader_cls.return_value = mock_downloader

            cohort._download_idc_files(threads=4, include=["CT"])

            mock_downloader_cls.assert_called_once_with(
                temp_dir,
                MAX_WORKERS=4,
                include=["CT"],
                exclude=None
            )
            mock_downloader.process_cases.assert_called_once()

    def test_download_tcia_files(self, sample_cohort_data, temp_dir):
        """Test TCIA file download method (deprecated)."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.TCIAFileDownloader') as mock_downloader_cls:
            mock_downloader = MagicMock()
            mock_downloader_cls.return_value = mock_downloader

            cohort._download_tcia_files(threads=4)

            mock_downloader_cls.assert_called_once_with(
                temp_dir,
                MAX_WORKERS=4,
                include=None,
                exclude=None
            )
            mock_downloader.process_cases.assert_called_once()

    def test_include_method(self, sample_cohort_data, temp_dir, capsys):
        """Test include method prints message."""
        cohort = Cohort(sample_cohort_data, temp_dir)

        cohort.include(["CT", "MR"])

        captured = capsys.readouterr()
        assert "CT" in captured.out
        assert "MR" in captured.out

    def test_stats_handles_missing_file_size(self, sample_cohort_data, temp_dir):
        """Test that stats handles entries without file_size gracefully."""
        manifest = [
            {
                "PatientID": "TCGA-01",
                "Clinical Supplement": [
                    {"file_name": "test.txt"}  # No file_size
                ]
            }
        ]

        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('builtins.open', mock_open(read_data=json.dumps(manifest))):
            stats = cohort.stats()

            assert "Clinical Supplement" in stats
            assert stats["Clinical Supplement"]["file_count"] == 1
            assert stats["Clinical Supplement"]["total_size"] == 0

    def test_stats_sorts_by_size(self, sample_cohort_data, temp_dir):
        """Test that stats returns modalities sorted by total size."""
        manifest = [
            {
                "PatientID": "TCGA-01",
                "Small Data": [{"file_size": 100}],
                "Large Data": [{"file_size": 10000}],
                "Medium Data": [{"file_size": 1000}]
            }
        ]

        cohort = Cohort(sample_cohort_data, temp_dir)

        with patch('builtins.open', mock_open(read_data=json.dumps(manifest))):
            stats = cohort.stats()

            # Should be sorted by size (descending)
            modalities = list(stats.keys())
            assert modalities[0] == "Large Data"
            assert modalities[1] == "Medium Data"
            assert modalities[2] == "Small Data"
