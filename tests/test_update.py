"""
Tests for the MINDSUpdater class.
"""
import os
import tarfile
import pytest
import responses
from unittest.mock import Mock, MagicMock, patch, mock_open
from app.med_minds.update import MINDSUpdater, StatusManager, DummyStatus


class TestMINDSUpdater:
    """Tests for MINDSUpdater class."""

    def test_init(self):
        """Test MINDSUpdater initialization."""
        updater = MINDSUpdater()

        assert updater.CLINICAL_URL == "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar"
        assert updater.BIOSPECIMEN_URL == "https://portal.gdc.cancer.gov/auth/api/v0/biospecimen_tar"
        assert updater.session is not None
        assert updater.temp_folder.endswith("/tmp")

    def test_build_data(self):
        """Test data payload building."""
        updater = MINDSUpdater()
        data = updater.build_data()

        assert data['size'] == 100_000
        assert data['attachment'] is True
        assert data['format'] == 'TSV'
        assert 'filters' in data

    @patch('app.med_minds.update.os.path.exists')
    @patch('app.med_minds.update.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @responses.activate
    def test_download_with_progress_success(self, mock_file, mock_getsize, mock_exists):
        """Test successful file download with progress."""
        mock_exists.return_value = False
        mock_getsize.return_value = 1024 * 1024  # 1 MB

        # Mock the API response
        responses.add(
            responses.POST,
            "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar",
            body=b"test data content",
            status=200,
            headers={'content-length': '17'}
        )

        updater = MINDSUpdater()
        mock_progress = MagicMock()
        mock_task = MagicMock()

        result = updater.download_with_progress(
            updater.CLINICAL_URL,
            "test_file.tar.gz",
            mock_progress,
            mock_task
        )

        # Verify progress was updated
        assert mock_progress.update.called

    @patch('app.med_minds.update.os.path.exists')
    def test_download_with_progress_file_exists(self, mock_exists):
        """Test download when file already exists."""
        mock_exists.return_value = True

        updater = MINDSUpdater()
        mock_progress = MagicMock()
        mock_task = MagicMock()

        updater.download_with_progress(
            updater.CLINICAL_URL,
            "existing_file.tar.gz",
            mock_progress,
            mock_task
        )

        # Should update progress to indicate file exists
        mock_progress.update.assert_called()

    @patch('app.med_minds.update.tarfile.open')
    @patch('app.med_minds.update.os.remove')
    @patch('app.med_minds.update.StatusManager.status')
    def test_extract(self, mock_status_manager, mock_remove, mock_tarfile):
        """Test tarball extraction."""
        # Mock tarfile operations
        mock_tar = MagicMock()
        mock_tarfile.return_value = mock_tar

        # Mock status context manager
        mock_status = MagicMock()
        mock_status_manager.return_value.__enter__.return_value = mock_status
        mock_status_manager.return_value.__exit__.return_value = None

        updater = MINDSUpdater()
        updater.extract()

        # Verify tarballs were opened and extracted
        assert mock_tarfile.call_count == 2
        assert mock_tar.extractall.call_count == 2
        assert mock_tar.close.call_count == 2

        # Verify cleanup happened
        assert mock_remove.call_count == 2

    def test_get_temp_folder(self):
        """Test getting temp folder path."""
        updater = MINDSUpdater()
        temp_folder = updater.get_temp_folder()

        assert temp_folder.endswith("/tmp")

    @patch('app.med_minds.update.os.makedirs')
    @patch('app.med_minds.update.ThreadPoolExecutor')
    @patch('app.med_minds.update.Progress')
    @patch.object(MINDSUpdater, 'extract')
    def test_threaded_update(self, mock_extract, mock_progress_cls, mock_executor_cls, mock_makedirs):
        """Test threaded update process."""
        # Mock Progress context manager
        mock_progress = MagicMock()
        mock_progress_cls.return_value.__enter__.return_value = mock_progress
        mock_progress_cls.return_value.__exit__.return_value = None

        # Mock ThreadPoolExecutor
        mock_executor = MagicMock()
        mock_executor_cls.return_value.__enter__.return_value = mock_executor

        # Mock futures
        mock_clinical_future = MagicMock()
        mock_biospecimen_future = MagicMock()
        mock_executor.submit.side_effect = [mock_clinical_future, mock_biospecimen_future]

        updater = MINDSUpdater()
        updater.threaded_update()

        # Verify temp folder was created
        mock_makedirs.assert_called_once()

        # Verify two tasks were submitted (clinical and biospecimen)
        assert mock_executor.submit.call_count == 2

        # Verify extract was called
        mock_extract.assert_called_once()

    @patch('app.med_minds.update.os.path.exists')
    @patch('app.med_minds.update.os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @responses.activate
    def test_download_retry_on_failure(self, mock_file, mock_getsize, mock_exists):
        """Test that download retries on failure."""
        mock_exists.return_value = False

        # First two attempts fail, third succeeds
        responses.add(
            responses.POST,
            "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar",
            status=500
        )
        responses.add(
            responses.POST,
            "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar",
            status=500
        )
        responses.add(
            responses.POST,
            "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar",
            body=b"test data",
            status=200,
            headers={'content-length': '9'}
        )

        mock_getsize.return_value = 9

        updater = MINDSUpdater()
        mock_progress = MagicMock()
        mock_task = MagicMock()

        # Should succeed after retries
        result = updater.download_with_progress(
            updater.CLINICAL_URL,
            "test_file.tar.gz",
            mock_progress,
            mock_task
        )

        # Should have made 3 attempts
        assert len(responses.calls) == 3

    @patch('app.med_minds.update.os.path.exists')
    @responses.activate
    def test_download_fails_after_retries(self, mock_exists):
        """Test that download raises exception after all retries fail."""
        mock_exists.return_value = False

        # All attempts fail
        for _ in range(3):
            responses.add(
                responses.POST,
                "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar",
                status=500
            )

        updater = MINDSUpdater()
        mock_progress = MagicMock()
        mock_task = MagicMock()

        # Should raise exception after retries
        with pytest.raises(Exception):
            updater.download_with_progress(
                updater.CLINICAL_URL,
                "test_file.tar.gz",
                mock_progress,
                mock_task
            )


class TestStatusManagerUpdate:
    """Tests for StatusManager in update module."""

    def test_status_manager_thread_safe(self):
        """Test that StatusManager is thread-safe."""
        StatusManager._active = False

        with patch('app.med_minds.update.Status') as mock_status:
            wrapper1 = StatusManager.status("test1")
            assert StatusManager._active is True

            # Second call should return DummyStatus
            wrapper2 = StatusManager.status("test2")
            assert isinstance(wrapper2, DummyStatus)

            # Reset
            StatusManager._active = False
