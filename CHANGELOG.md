# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-01-19

### Added

#### Comprehensive Test Suite
- Added complete test coverage using pytest framework
- **test_database.py**: DatabaseManager tests (connection, queries, schema operations)
- **test_update.py**: MINDSUpdater tests (GDC downloads, extraction)
- **test_aggregator.py**: Aggregator tests (manifest generation, API calls)
- **test_downloader.py**: Downloader tests (GDC, IDC, TCIA file downloads)
- **test_cohort.py**: Cohort class tests (initialization, downloads, stats)
- **test_api.py**: Main API tests (query, get_tables, build_cohort, etc.)
- **test_integration.py**: End-to-end workflow tests
- **conftest.py**: Shared fixtures and test configuration
- **tests/README.md**: Comprehensive test documentation
- Test dependencies added to pyproject.toml: pytest, pytest-cov, pytest-mock, responses

#### IDC (Imaging Data Commons) v2 API Support
- Added **IDCFileDownloader** class with full v2 API support
- Implemented pagination support using `next_page` token for large result sets
- Added `idc_files()` method to Aggregator for querying IDC v2 API
- Added modality filtering support (include/exclude) for IDC downloads
- Support for IDC-specific metadata fields:
  - `collection_id` (lowercase, replaces Collection_ID)
  - `PatientID` (replaces Patient_ID)
  - `gcs_url` (lowercase, replaces GCS_URL)
  - `crdc_series_uuid` (replaces CRDC_Series_GUID)
  - `source: "IDC"` field to identify IDC data
- Updated manifest format to handle both old and new field names for backward compatibility
- Changed default download behavior to use IDC (`use_idc=True` by default)

### Changed

#### TCIA Deprecation
- **DEPRECATED**: TCIA (The Cancer Imaging Archive) support is now deprecated
- Added deprecation warnings to `TCIAFileDownloader` class
- Added deprecation warnings to `tcia_files()` method in Aggregator
- TCIA is no longer hosting controlled-access data due to [NIH policy changes](https://www.cancerimagingarchive.net/new-nih-policies-for-controlled-access-data/)
- Users are encouraged to migrate to IDC for all imaging data access

#### Improved Error Handling
- Enhanced `Cohort.stats()` to handle missing `file_size` gracefully using `.get()` with default value of 0
- Removed try-except block in stats calculation, replaced with safer `.get()` method
- Better handling of numpy data types in JSON serialization

#### Enhanced numpy Type Conversion
- Extended `numpy_to_python()` helper function in aggregator.py
- Added support for:
  - `np.ndarray` → list conversion
  - `np.integer`, `np.floating` → Python int/float conversion
  - `np.str_`, `np.bytes_` → Python string conversion
  - `np.bool_` → Python bool conversion
- Ensures all numpy types are properly converted before JSON serialization

#### Download System Improvements
- Updated `Cohort.download()` to accept `use_idc` parameter (default: True)
- Added deprecation warning when `use_idc=False` is used
- Modified `IDCFileDownloader.process_cases()` to read directly from manifest
- Removed redundant API calls during download phase
- Better field name handling for IDC v2 API compatibility

### Fixed

- Fixed JSON serialization errors caused by numpy data types in manifest generation
- Fixed patient_id handling in `tcia_files()` to support both list and string inputs
- Fixed manifest aggregation to properly merge GDC and IDC data sources
- Fixed API pagination logic to handle v2 API response format

### Documentation

#### README Updates
- Added prominent TCIA migration notice
- Updated installation instructions
- Updated usage examples to reflect IDC as primary imaging data source
- Added note about NIH policy changes affecting TCIA

#### Code Documentation
- Added docstrings to `Cohort.download()` method explaining parameters
- Added deprecation notices to TCIA-related classes and methods
- Updated inline comments to reflect IDC v2 API changes
- Enhanced class-level documentation for IDCFileDownloader

## [0.1.0] - 2024-10-15

### Changed
- Migrated from MySQL to PostgreSQL as database backend
- Updated database connection to use psycopg2-binary
- Updated schema queries to use PostgreSQL information_schema
- Sanitized column names (dots, spaces, hyphens → underscores)
- Updated dependencies in pyproject.toml

### Documentation
- Added PostgreSQL migration notes to README
- Updated environment setup instructions

## [0.0.8] - Previous Release

### Changed
- Updated package name to med-minds (from minds)
- Removed old distribution files

## [0.0.7] - Previous Release

### Added
- MINDSUpdater for automated data downloading from GDC
- Support for Python 3.11 and 3.12

### Changed
- Updated classifiers for broader Python version compatibility

[0.2.0]: https://github.com/lab-rasool/MINDS/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lab-rasool/MINDS/compare/v0.0.8...v0.1.0
[0.0.8]: https://github.com/lab-rasool/MINDS/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/lab-rasool/MINDS/releases/tag/v0.0.7
