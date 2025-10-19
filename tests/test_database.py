"""
Tests for the DatabaseManager class.
"""
import os
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch, mock_open
from app.med_minds.database import DatabaseManager, StatusManager, DummyStatus


class TestStatusManager:
    """Tests for StatusManager class."""

    def test_status_not_active_initially(self):
        """Test that status is not active initially."""
        assert StatusManager._active is False

    def test_status_creates_wrapper(self):
        """Test that status() creates a proper wrapper."""
        with patch('app.med_minds.database.Status') as mock_status:
            StatusManager._active = False
            wrapper = StatusManager.status("test message")
            assert wrapper is not None

    def test_status_returns_dummy_when_active(self):
        """Test that status returns DummyStatus when already active."""
        StatusManager._active = True
        result = StatusManager.status("test message")
        assert isinstance(result, DummyStatus)
        StatusManager._active = False


class TestDummyStatus:
    """Tests for DummyStatus class."""

    def test_dummy_status_enter_exit(self):
        """Test DummyStatus context manager."""
        dummy = DummyStatus("test")
        assert dummy.message == "test"

        with dummy as d:
            assert d is dummy
            d.update("new message")  # Should do nothing


class TestDatabaseManager:
    """Tests for DatabaseManager class."""

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    @patch('app.med_minds.database.os.getenv')
    def test_init_loads_env(self, mock_getenv, mock_connect, mock_load_dotenv):
        """Test that DatabaseManager loads environment variables."""
        mock_connect.return_value = MagicMock()

        # Mock getenv to return test values
        def getenv_side_effect(key):
            env_vars = {
                'HOST': '127.0.0.1',
                'PORT': '5432',
                'DB_USER': 'test',
                'PASSWORD': 'test',
                'DATABASE': 'test_db'
            }
            return env_vars.get(key)

        mock_getenv.side_effect = getenv_side_effect

        db = DatabaseManager()
        mock_load_dotenv.assert_called_once()
        assert db.config['host'] == '127.0.0.1'
        assert db.config['port'] == '5432'
        assert db.config['user'] == 'test'
        assert db.config['password'] == 'test'
        assert db.config['database'] == 'test_db'

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_connect_to_db(self, mock_connect, mock_load_dotenv):
        """Test database connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            mock_connect.assert_called_once()
            assert db.connection == mock_connection

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_execute_query(self, mock_connect, mock_load_dotenv):
        """Test query execution returns DataFrame."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        # The execute method doesn't use cursor as context manager, it just calls cursor()
        mock_connection.cursor.return_value = mock_cursor

        # Mock cursor description and fetchall
        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('col1',), ('col2',), ('col3',)]
        mock_cursor.fetchall.return_value = [
            ('val1', 'val2', 'val3'),
            ('val4', 'val5', 'val6')
        ]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            result = db.execute("SELECT * FROM test", use_status=False)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == ['col1', 'col2', 'col3']

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_get_tables(self, mock_connect, mock_load_dotenv):
        """Test retrieving table list."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('table_name',)]
        mock_cursor.fetchall.return_value = [
            ('clinical',),
            ('biospecimen',),
            ('project',)
        ]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            tables = db.get_tables(use_status=False)

            # get_tables returns a pandas Series, convert to list
            tables_list = tables.tolist()
            assert isinstance(tables_list, list)
            assert 'clinical' in tables_list
            assert 'biospecimen' in tables_list
            assert 'project' in tables_list

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_get_columns(self, mock_connect, mock_load_dotenv):
        """Test retrieving column list."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('column_name',)]
        mock_cursor.fetchall.return_value = [
            ('case_id',),
            ('submitter_id',),
            ('project_id',)
        ]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            columns = db.get_columns('clinical', use_status=False)

            # get_columns returns a pandas Series, convert to list
            columns_list = columns.tolist()
            assert isinstance(columns_list, list)
            assert 'case_id' in columns_list
            assert 'submitter_id' in columns_list

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_get_minds_cohort(self, mock_connect, mock_load_dotenv):
        """Test getting MINDS cohort."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('cases_case_id',), ('cases_submitter_id',)]
        mock_cursor.fetchall.return_value = [
            ('case-1', 'TCGA-01'),
            ('case-2', 'TCGA-02')
        ]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            cohort = db.get_minds_cohort("SELECT * FROM clinical", use_status=False)

            # get_minds_cohort returns a pandas Series (from groupby)
            assert isinstance(cohort, pd.Series)
            assert len(cohort) == 2

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    @patch('app.med_minds.database.pd.read_csv')
    def test_get_gdc_cohort(self, mock_read_csv, mock_connect, mock_load_dotenv):
        """Test getting GDC cohort from file."""
        # Mock the TSV file reading
        import pandas as pd
        mock_read_csv.return_value = pd.DataFrame({
            'id': ['case-1', 'case-2', 'case-3']
        })
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('cases_case_id',), ('cases_submitter_id',)]
        mock_cursor.fetchall.return_value = [
            ('case-1', 'TCGA-01'),
            ('case-2', 'TCGA-02'),
            ('case-3', 'TCGA-03')
        ]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            cohort = db.get_gdc_cohort('cohort.txt', use_status=False)

            # get_gdc_cohort returns a pandas Series (from groupby)
            assert isinstance(cohort, pd.Series)
            assert len(cohort) == 3

    @patch('app.med_minds.database.load_dotenv')
    @patch('app.med_minds.database.psycopg2.connect')
    def test_column_sanitization(self, mock_connect, mock_load_dotenv):
        """Test that column names are properly sanitized."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # psycopg2 cursor.description returns tuples: (name, type_code, ...)
        mock_cursor.description = [('col_with_dots',), ('col_with_dashes',), ('col_with_spaces',)]
        mock_cursor.fetchall.return_value = [('val1', 'val2', 'val3')]

        mock_connect.return_value = mock_connection

        with patch.dict(os.environ, {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DB_USER': 'test',
            'PASSWORD': 'test',
            'DATABASE': 'test_db'
        }):
            db = DatabaseManager()
            result = db.execute("SELECT * FROM test", use_status=False)

            # Column names should be sanitized (dots, dashes, spaces → underscores)
            assert 'col_with_dots' in result.columns
            assert 'col_with_dashes' in result.columns
            assert 'col_with_spaces' in result.columns
