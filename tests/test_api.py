"""
Tests for the main API functions in __init__.py.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
import app.med_minds as med_minds


class TestAPIFunctions:
    """Tests for main API functions."""

    @patch('app.med_minds.db')
    def test_get_tables(self, mock_db):
        """Test get_tables function."""
        mock_db.get_tables.return_value = ['clinical', 'biospecimen', 'project']

        result = med_minds.get_tables()

        assert result == ['clinical', 'biospecimen', 'project']
        mock_db.get_tables.assert_called_once()

    @patch('app.med_minds.db')
    def test_get_columns(self, mock_db):
        """Test get_columns function."""
        mock_db.get_columns.return_value = ['case_id', 'submitter_id', 'project_id']

        result = med_minds.get_columns('clinical')

        assert result == ['case_id', 'submitter_id', 'project_id']
        mock_db.get_columns.assert_called_once_with('clinical')

    @patch('app.med_minds.db')
    def test_query(self, mock_db, sample_cohort_data):
        """Test query function."""
        mock_db.execute.return_value = sample_cohort_data

        result = med_minds.query("SELECT * FROM clinical")

        assert isinstance(result, pd.DataFrame)
        assert result.equals(sample_cohort_data)
        mock_db.execute.assert_called_once_with("SELECT * FROM clinical")

    @patch('app.med_minds.db')
    def test_build_cohort_with_query(self, mock_db, sample_cohort_data, temp_dir):
        """Test build_cohort with SQL query."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Cohort') as mock_cohort_cls:
            mock_cohort = MagicMock()
            mock_cohort_cls.return_value = mock_cohort

            result = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical WHERE project='TCGA-LUAD'"
            )

            mock_db.get_minds_cohort.assert_called_once_with(
                "SELECT * FROM clinical WHERE project='TCGA-LUAD'"
            )
            mock_cohort_cls.assert_called_once()
            mock_cohort.generate_manifest.assert_called_once()

    @patch('app.med_minds.db')
    def test_build_cohort_with_gdc_file(self, mock_db, sample_cohort_data, temp_dir):
        """Test build_cohort with GDC cohort file."""
        mock_db.get_gdc_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Cohort') as mock_cohort_cls:
            mock_cohort = MagicMock()
            mock_cohort_cls.return_value = mock_cohort

            result = med_minds.build_cohort(
                output_dir=temp_dir,
                gdc_cohort="cohort.txt"
            )

            mock_db.get_gdc_cohort.assert_called_once_with("cohort.txt")
            mock_cohort_cls.assert_called_once()
            mock_cohort.generate_manifest.assert_called_once()

    def test_build_cohort_without_query_or_file_raises_error(self, temp_dir):
        """Test that build_cohort raises error without query or file."""
        with pytest.raises(ValueError) as exc_info:
            med_minds.build_cohort(output_dir=temp_dir)

        assert "Either a query or a gdc_cohort file must be provided" in str(exc_info.value)

    @patch('app.med_minds.db')
    def test_build_cohort_with_manifest_false(self, mock_db, sample_cohort_data, temp_dir):
        """Test build_cohort with manifest=False doesn't generate manifest."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Cohort') as mock_cohort_cls:
            mock_cohort = MagicMock()
            mock_cohort_cls.return_value = mock_cohort

            result = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical",
                manifest=False
            )

            # Should not call generate_manifest
            mock_cohort.generate_manifest.assert_not_called()

    @patch('app.med_minds.MINDSUpdater')
    @patch('app.med_minds.db')
    def test_update(self, mock_db, mock_updater_cls):
        """Test update function."""
        mock_updater = MagicMock()
        mock_updater.temp_folder = "/tmp/minds"
        mock_updater_cls.return_value = mock_updater

        med_minds.update()

        mock_updater_cls.assert_called_once()
        mock_updater.threaded_update.assert_called_once()
        mock_db.update.assert_called_once_with("/tmp/minds")

    @patch('app.med_minds.MINDSUpdater')
    @patch('app.med_minds.db')
    def test_update_skip_download(self, mock_db, mock_updater_cls):
        """Test update function with skip_download=True."""
        mock_updater = MagicMock()
        mock_updater.temp_folder = "/tmp/minds"
        mock_updater_cls.return_value = mock_updater

        med_minds.update(skip_download=True)

        mock_updater_cls.assert_called_once()
        # Should not call threaded_update when skip_download=True
        mock_updater.threaded_update.assert_not_called()
        mock_db.update.assert_called_once_with("/tmp/minds")


class TestModuleInitialization:
    """Tests for module-level initialization."""

    def test_db_initialized(self):
        """Test that db is initialized."""
        assert med_minds.db is not None

    def test_console_initialized(self):
        """Test that console is initialized."""
        assert med_minds.console is not None

    def test_cohort_class_available(self):
        """Test that Cohort class is available."""
        assert hasattr(med_minds, 'Cohort')

    def test_downloader_classes_available(self):
        """Test that downloader classes are available."""
        assert hasattr(med_minds, 'GDCFileDownloader')
        assert hasattr(med_minds, 'IDCFileDownloader')
        assert hasattr(med_minds, 'TCIAFileDownloader')


class TestCohortCreation:
    """Tests for creating Cohort instances."""

    def test_cohort_instance_creation(self, sample_cohort_data, temp_dir):
        """Test creating a Cohort instance directly."""
        cohort = med_minds.Cohort(sample_cohort_data, temp_dir)

        assert cohort.data.equals(sample_cohort_data)
        assert cohort.output_dir == temp_dir

    @patch('app.med_minds.db')
    def test_build_cohort_returns_cohort_instance(self, mock_db, sample_cohort_data, temp_dir):
        """Test that build_cohort returns a Cohort instance."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        cohort = med_minds.build_cohort(
            output_dir=temp_dir,
            query="SELECT * FROM clinical"
        )

        assert isinstance(cohort, med_minds.Cohort)

    @patch('app.med_minds.db')
    def test_cohort_workflow(self, mock_db, sample_cohort_data, temp_dir, sample_manifest_file):
        """Test complete cohort workflow."""
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        # Build cohort
        with patch('app.med_minds.Aggregator') as mock_aggregator_cls:
            mock_aggregator = MagicMock()
            mock_aggregator_cls.return_value = mock_aggregator

            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical WHERE project='TCGA-LUAD'"
            )

            # Verify cohort was created
            assert isinstance(cohort, med_minds.Cohort)

            # Verify manifest generation was called
            mock_aggregator.generate_manifest.assert_called_once()


class TestAPIIntegration:
    """Integration tests for API functions working together."""

    @patch('app.med_minds.db')
    def test_query_to_cohort_workflow(self, mock_db, sample_cohort_data, temp_dir):
        """Test workflow from query to cohort creation."""
        # Step 1: Query database
        mock_db.execute.return_value = sample_cohort_data
        query_result = med_minds.query("SELECT * FROM clinical")

        assert isinstance(query_result, pd.DataFrame)

        # Step 2: Build cohort from query
        mock_db.get_minds_cohort.return_value = sample_cohort_data

        with patch('app.med_minds.Aggregator'):
            cohort = med_minds.build_cohort(
                output_dir=temp_dir,
                query="SELECT * FROM clinical"
            )

            assert isinstance(cohort, med_minds.Cohort)

    @patch('app.med_minds.db')
    @patch('app.med_minds.MINDSUpdater')
    def test_update_then_query_workflow(self, mock_updater_cls, mock_db, sample_cohort_data):
        """Test workflow of updating database then querying."""
        # Step 1: Update database
        mock_updater = MagicMock()
        mock_updater.temp_folder = "/tmp/minds"
        mock_updater_cls.return_value = mock_updater

        med_minds.update()

        mock_updater.threaded_update.assert_called_once()
        mock_db.update.assert_called_once()

        # Step 2: Query updated database
        mock_db.execute.return_value = sample_cohort_data
        result = med_minds.query("SELECT * FROM clinical")

        assert isinstance(result, pd.DataFrame)
