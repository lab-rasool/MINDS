"""
Integration tests for end-to-end workflows in MINDS.
"""
import json
import os
import pytest
import pandas as pd
import responses
from unittest.mock import Mock, MagicMock, patch, mock_open
import app.med_minds as med_minds


class TestEndToEndWorkflow:
    """End-to-end integration tests."""

    @patch('app.med_minds.MINDSUpdater')
    @patch('app.med_minds.db')
    def test_complete_workflow_update_to_download(
        self, mock_db, mock_updater_cls, sample_cohort_data, temp_dir, sample_manifest_file
    ):
        """Test complete workflow: update -> query -> build_cohort -> download."""

        # Step 1: Update database
        mock_updater = MagicMock()
        mock_updater.temp_folder = "/tmp/minds"
        mock_updater_cls.return_value = mock_updater

        med_minds.update()

        mock_updater.threaded_update.assert_called_once()
        mock_db.update.assert_called_once()

        # Step 2: Query database
        mock_db.execute.return_value = sample_cohort_data
        query_result = med_minds.query("SELECT * FROM clinical WHERE project='TCGA-LUAD'")

        assert isinstance(query_result, pd.DataFrame)
        assert len(query_result) == 3

        # Step 3: Build cohort
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator') as mock_aggregator_cls:
            mock_aggregator = MagicMock()
            mock_aggregator_cls.return_value = mock_aggregator

            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical WHERE project='TCGA-LUAD'"
            )

            assert isinstance(cohort, med_minds.Cohort)
            mock_aggregator.generate_manifest.assert_called_once()

        # Step 4: Download files
        cohort.manifest_file = sample_manifest_file

        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files') as mock_gdc, \
             patch.object(cohort, '_download_idc_files') as mock_idc:
            mock_exists.return_value = True

            cohort.download(threads=4)

            mock_gdc.assert_called_once()
            mock_idc.assert_called_once()

    @patch('app.med_minds.db')
    def test_gdc_cohort_file_workflow(
        self, mock_db, sample_cohort_data, temp_dir
    ):
        """Test workflow starting from GDC cohort file."""

        # Build cohort from GDC file
        mock_db.get_gdc_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator') as mock_aggregator_cls:
            mock_aggregator = MagicMock()
            mock_aggregator_cls.return_value = mock_aggregator

            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                gdc_cohort="cohort.tsv"
            )

            assert isinstance(cohort, med_minds.Cohort)
            mock_db.get_gdc_cohort.assert_called_once_with("cohort.tsv")
            mock_aggregator.generate_manifest.assert_called_once()

    @patch('app.med_minds.db')
    def test_cohort_stats_workflow(
        self, mock_db, sample_cohort_data, temp_dir, sample_manifest
    ):
        """Test workflow for building cohort and viewing stats."""

        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator'):
            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

        # View stats
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_manifest))):
            stats = cohort.stats()

            assert isinstance(stats, dict)
            assert len(stats) > 0

    @patch('app.med_minds.db')
    def test_filtered_download_workflow(
        self, mock_db, sample_cohort_data, temp_dir, sample_manifest_file
    ):
        """Test workflow with filtered downloads (include/exclude)."""

        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator'):
            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

        cohort.manifest_file = sample_manifest_file

        # Download with filters
        with patch('app.med_minds.os.path.exists') as mock_exists, \
             patch.object(cohort, '_download_gdc_files') as mock_gdc, \
             patch.object(cohort, '_download_idc_files') as mock_idc:
            mock_exists.return_value = True

            # Test include filter
            cohort.download(threads=4, include=["CT", "MR"])
            mock_gdc.assert_called_with(4, include=["CT", "MR"], exclude=None)

            # Test exclude filter
            cohort.download(threads=4, exclude=["Slide Image"])
            mock_gdc.assert_called_with(4, include=None, exclude=["Slide Image"])


class TestCrossModuleIntegration:
    """Tests for interactions between different modules."""

    @patch('app.med_minds.db')
    @responses.activate
    def test_aggregator_to_downloader_integration(
        self, mock_db, sample_cohort_data, temp_dir, sample_gdc_response
    ):
        """Test data flow from Aggregator to Downloader."""

        # Mock GDC API
        responses.add(
            responses.GET,
            "https://api.gdc.cancer.gov/files",
            json=sample_gdc_response,
            status=200
        )

        mock_db.get_minds_cohort.return_value = sample_cohort_data

        # Create cohort and generate manifest
        with patch('app.med_minds.Aggregator.generate_manifest') as mock_generate:
            # Simulate manifest generation
            manifest_path = os.path.join(temp_dir, "manifest.json")
            sample_manifest = [{
                "PatientID": "TCGA-01",
                "gdc_case_id": "case-1",
                "Clinical Supplement": [
                    {"id": "file-1", "file_name": "clinical.txt", "file_size": 1024}
                ]
            }]

            with patch('builtins.open', mock_open()) as mock_file:
                with patch('json.dump') as mock_json_dump:
                    cohort = med_minds.build_cohort(
                        output_dir=temp_dir,
                        query="SELECT * FROM clinical"
                    )

        # Verify downloader can read the manifest
        with patch('builtins.open', mock_open(read_data=json.dumps(sample_manifest))):
            downloader = med_minds.GDCFileDownloader(temp_dir, MAX_WORKERS=4)
            assert len(downloader.manifest) > 0

    @patch('app.med_minds.db')
    def test_database_to_cohort_integration(
        self, mock_db, sample_cohort_data, temp_dir
    ):
        """Test data flow from Database to Cohort."""

        # Database query returns DataFrame
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator'):
            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

            # Cohort should have the data from database
            assert cohort.data.equals(sample_cohort_data)

    @patch('app.med_minds.MINDSUpdater')
    @patch('app.med_minds.db')
    def test_updater_to_database_integration(self, mock_db, mock_updater_cls):
        """Test data flow from MINDSUpdater to DatabaseManager."""

        mock_updater = MagicMock()
        mock_updater.temp_folder = "/tmp/minds"
        mock_updater_cls.return_value = mock_updater

        med_minds.update()

        # Verify data flows from updater to database
        mock_updater.threaded_update.assert_called_once()
        mock_db.update.assert_called_once_with("/tmp/minds")


class TestErrorHandling:
    """Test error handling across modules."""

    def test_missing_manifest_error(self, sample_cohort_data, temp_dir):
        """Test error when manifest is missing."""
        cohort = med_minds.Cohort(sample_cohort_data, temp_dir)

        with patch('app.med_minds.os.path.exists') as mock_exists:
            mock_exists.return_value = False

            with pytest.raises(FileNotFoundError) as exc_info:
                cohort.download()

            assert "No manifest file found" in str(exc_info.value)

    def test_invalid_cohort_parameters(self, temp_dir):
        """Test error with invalid cohort parameters."""
        with pytest.raises(ValueError) as exc_info:
            med_minds.build_cohort(output_dir=temp_dir)

        assert "Either a query or a gdc_cohort file must be provided" in str(exc_info.value)

    @patch('app.med_minds.db')
    @responses.activate
    def test_api_failure_handling(self, mock_db, sample_cohort_data, temp_dir):
        """Test handling of API failures."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        # Mock API failures
        for _ in range(5):
            responses.add(
                responses.GET,
                "https://api.gdc.cancer.gov/files",
                status=500
            )

        from app.med_minds.aggregator import Aggregator

        aggregator = Aggregator(sample_cohort_data, temp_dir)

        # Should retry and eventually fail or handle gracefully
        with pytest.raises(Exception):
            aggregator.gdc_files('case-1', ['TCGA-01'])


class TestDataConsistency:
    """Tests for data consistency across the workflow."""

    @patch('app.med_minds.db')
    def test_case_id_consistency(self, mock_db, sample_cohort_data, temp_dir):
        """Test that case IDs remain consistent throughout workflow."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator') as mock_aggregator_cls:
            mock_aggregator = MagicMock()
            mock_aggregator_cls.return_value = mock_aggregator

            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

            # Verify aggregator received correct case IDs
            call_args = mock_aggregator_cls.call_args
            passed_data = call_args[0][0]

            assert passed_data.equals(sample_cohort_data)

    @patch('app.med_minds.db')
    def test_manifest_data_consistency(self, mock_db, sample_cohort_data, temp_dir):
        """Test that manifest data matches source cohort."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        manifest_data = []

        def capture_manifest(*args, **kwargs):
            # Capture what would be written to manifest
            nonlocal manifest_data
            manifest_data = args[0] if args else []

        with patch('app.med_minds.Aggregator.generate_manifest') as mock_generate:
            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

            # Verify cohort has correct patient data
            case_ids = cohort.data.index.tolist()
            assert case_ids == ['case-1', 'case-2', 'case-3']


class TestPerformance:
    """Performance and concurrency tests."""

    @patch('app.med_minds.db')
    def test_parallel_processing(self, mock_db, temp_dir):
        """Test that parallel processing is used where expected."""

        # Create larger cohort
        large_cohort = pd.DataFrame({
            'case_id': [f'case-{i}' for i in range(100)],
            'submitter_id': [f'TCGA-{i:02d}' for i in range(100)],
        }).set_index('case_id')

        mock_db.get_minds_cohort.return_value = large_cohort

        with patch('app.med_minds.aggregator.ThreadPoolExecutor') as mock_executor_cls:
            mock_executor = MagicMock()
            mock_executor_cls.return_value.__enter__.return_value = mock_executor
            mock_executor_cls.return_value.__exit__.return_value = None

            with patch('app.med_minds.aggregator.Progress'):
                from app.med_minds.aggregator import Aggregator

                aggregator = Aggregator(large_cohort, temp_dir, max_workers=8)

                # Verify max_workers is respected
                assert aggregator.MAX_WORKERS == 8
