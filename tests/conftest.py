"""
Shared fixtures and configuration for MINDS tests.
"""
import os
import json
import tempfile
from unittest.mock import Mock, MagicMock
import pytest
import pandas as pd


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_env_file(tmp_path):
    """Create a temporary .env file for testing."""
    env_file = tmp_path / ".env"
    env_content = """
HOST=127.0.0.1
PORT=5432
DB_USER=test_user
PASSWORD=test_password
DATABASE=test_db
"""
    env_file.write_text(env_content)
    return str(env_file)


@pytest.fixture
def sample_cohort_data():
    """Sample cohort DataFrame for testing."""
    return pd.DataFrame({
        'case_id': ['case-1', 'case-2', 'case-3'],
        'submitter_id': ['TCGA-01', 'TCGA-02', 'TCGA-03'],
        'project': ['TCGA-LUAD', 'TCGA-LUAD', 'TCGA-BRCA']
    }).set_index('case_id')


@pytest.fixture
def sample_case_ids():
    """Sample case IDs for testing."""
    return ['case-1', 'case-2', 'case-3']


@pytest.fixture
def sample_submitter_ids():
    """Sample submitter IDs for testing."""
    return [['TCGA-01'], ['TCGA-02'], ['TCGA-03']]


@pytest.fixture
def sample_gdc_response():
    """Sample GDC API response for testing."""
    return {
        "data": {
            "hits": [
                {
                    "id": "file-uuid-1",
                    "file_name": "sample1.bam",
                    "file_size": 1024000,
                    "data_type": "Aligned Reads",
                    "data_category": "Sequencing Reads",
                    "data_format": "BAM",
                    "access": "open",
                    "md5sum": "abc123"
                },
                {
                    "id": "file-uuid-2",
                    "file_name": "sample2.txt",
                    "file_size": 2048,
                    "data_type": "Clinical Supplement",
                    "data_category": "Clinical",
                    "data_format": "TXT",
                    "access": "open",
                    "md5sum": "def456"
                }
            ]
        }
    }


@pytest.fixture
def sample_idc_response():
    """Sample IDC API response for testing."""
    return {
        "series_manifest": [
            {
                "PatientID": "TCGA-01",
                "StudyInstanceUID": "study-uid-1",
                "SeriesInstanceUID": "series-uid-1",
                "Modality": "CT",
                "GCS_URL": "gs://bucket/path/to/file1.dcm",
                "Collection_ID": "TCGA-LUAD",
                "crdc_series_uuid": "idc-series-1"
            },
            {
                "PatientID": "TCGA-01",
                "StudyInstanceUID": "study-uid-2",
                "SeriesInstanceUID": "series-uid-2",
                "Modality": "MR",
                "GCS_URL": "gs://bucket/path/to/file2.dcm",
                "Collection_ID": "TCGA-LUAD",
                "crdc_series_uuid": "idc-series-2"
            }
        ]
    }


@pytest.fixture
def sample_manifest():
    """Sample manifest data for testing."""
    return [
        {
            "PatientID": "TCGA-01",
            "Aligned Reads": [
                {
                    "file_id": "file-uuid-1",
                    "file_name": "sample1.bam",
                    "file_size": 1024000,
                    "data_type": "Aligned Reads",
                    "md5sum": "abc123"
                }
            ],
            "CT": [
                {
                    "SeriesInstanceUID": "series-uid-1",
                    "GCS_URL": "gs://bucket/path/to/file1.dcm",
                    "Modality": "CT",
                    "Collection_ID": "TCGA-LUAD",
                    "source": "IDC"
                }
            ]
        }
    ]


@pytest.fixture
def sample_manifest_file(temp_dir, sample_manifest):
    """Create a sample manifest.json file."""
    manifest_path = os.path.join(temp_dir, "manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(sample_manifest, f)
    return manifest_path


@pytest.fixture
def mock_db_connection():
    """Mock psycopg2 database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_db_cursor():
    """Mock database cursor."""
    return MagicMock()


@pytest.fixture
def sample_db_tables():
    """Sample database tables list."""
    return ['clinical', 'biospecimen', 'project']


@pytest.fixture
def sample_db_columns():
    """Sample database columns."""
    return ['case_id', 'submitter_id', 'project_id', 'disease_type']


@pytest.fixture
def mock_requests_get():
    """Mock requests.get for API calls."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"hits": []}}
    return mock_response


@pytest.fixture(autouse=True)
def reset_env_vars():
    """Reset environment variables before and after each test."""
    # Store original env vars
    original_env = {
        'HOST': os.environ.get('HOST'),
        'PORT': os.environ.get('PORT'),
        'DB_USER': os.environ.get('DB_USER'),
        'PASSWORD': os.environ.get('PASSWORD'),
        'DATABASE': os.environ.get('DATABASE')
    }

    yield

    # Restore original env vars
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
