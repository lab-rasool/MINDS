import datetime
import os
import tarfile
from concurrent.futures import ThreadPoolExecutor

import requests
import retry
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TransferSpeedColumn,
)
from rich.status import Status

console = Console()


class StatusManager:
    """Thread-safe manager for Rich status displays"""

    _active = False
    _lock = __import__(
        "threading"
    ).Lock()  # Import threading within the class to avoid extra imports

    @classmethod
    def status(cls, message, spinner="dots"):
        """Create a status display that won't conflict with others"""
        with cls._lock:
            if cls._active:
                # Return a dummy context manager if a status is already active
                return DummyStatus(message)
            else:
                status = Status(message, spinner=spinner, console=console)
                cls._active = True

        class StatusWrapper:
            def __enter__(self):
                status.__enter__()
                return status

            def __exit__(self, exc_type, exc_val, exc_tb):
                with StatusManager._lock:
                    StatusManager._active = False
                return status.__exit__(exc_type, exc_val, exc_tb)

        return StatusWrapper()


class DummyStatus:
    """A dummy status that does nothing to prevent errors when nested"""

    def __init__(self, message):
        self.message = message
        console.print(f"[dim]{message}[/dim]")  # Print the message without animation

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def update(self, message):
        console.print(f"[dim]{message}[/dim]")  # Print updates without animation


class MINDSUpdater:
    """
    MINDSUpdater - A utility class for downloading and extracting clinical and biospecimen data from GDC.

    This class manages the entire process of retrieving medical data from the GDC (Genomic Data Commons)
    cancer portal, including downloading tarball files with progress reporting and extracting them
    to a temporary directory for further processing.

    Attributes:
        CLINICAL_URL (str): API endpoint for clinical data.
        BIOSPECIMEN_URL (str): API endpoint for biospecimen data.
        session (requests.Session): HTTP session for making requests.
        today (str): Current date in YYYY-MM-DD format.
        clinical_tar_file (str): Filename for the clinical data tarball.
        biospecimen_tar_file (str): Filename for the biospecimen data tarball.
        temp_folder (str): Path to the temporary folder for extracted files.

    Methods:
        build_data(): Builds the API request payload.
        download_with_progress(url, file_name, progress, task): Downloads a file with progress reporting.
        extract(): Extracts downloaded tarballs to the temp folder.
        get_temp_folder(): Returns the path to the temporary folder.
        threaded_update(): Main method to download and extract files using parallel threads.

    Example:
        ```python
        updater = MINDSUpdater()
        updater.threaded_update()
        temp_folder = updater.get_temp_folder()
        # Process extracted files in temp_folder
        ```
    """

    def __init__(self):
        self.CLINICAL_URL = "https://portal.gdc.cancer.gov/auth/api/v0/clinical_tar"
        self.BIOSPECIMEN_URL = (
            "https://portal.gdc.cancer.gov/auth/api/v0/biospecimen_tar"
        )
        self.session = requests.Session()
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.clinical_tar_file = f"clinical.cases_selection.{self.today}.tar.gz"
        self.biospecimen_tar_file = f"biospecimen.cases_selection.{self.today}.tar.gz"
        self.temp_folder = os.getcwd() + "/tmp"

    def build_data(self):
        size = 100_000  # TODO: get this number from the GDC API
        filters = '{"op":"and","content":[{"op":"in","content":{"field":"files.access","value":["open"]}}]}'
        return {
            "size": size,
            "attachment": True,
            "format": "TSV",
            "filters": filters,
        }

    @retry.retry(tries=3, delay=2, backoff=2, jitter=(1, 3))
    def download_with_progress(self, url, file_name, progress, task):
        """Download files with progress reporting"""
        full_path = os.path.join(file_name)

        if os.path.exists(full_path):
            progress.update(task, description=f"[yellow]{file_name} (already exists)")
            progress.update(task, completed=100, total=100)  # Mark as complete
            return

        try:
            # Update status to indicate request is being prepared
            progress.update(task, description=f"[cyan]Requesting {file_name}...")

            # Prepare the data for the request
            data = self.build_data()

            # Make the request with streaming enabled
            response = self.session.post(url, data=data, stream=True)
            response.raise_for_status()

            # Get content length if available, otherwise use indeterminate progress
            total_size = int(response.headers.get("content-length", 0))

            if total_size == 0:
                # No content length header, use indeterminate progress
                progress.update(
                    task, description=f"[cyan]Downloading {file_name} (size unknown)"
                )
            else:
                # Update progress with total size
                progress.update(
                    task, total=total_size, description=f"[cyan]Downloading {file_name}"
                )
                console.print(
                    f"[dim]Download size: {total_size / 1024 / 1024:.2f} MB[/dim]"
                )

            # Download the file with progress updates
            with open(full_path, "wb") as f:
                downloaded = 0
                for chunk in response.iter_content(
                    chunk_size=8192
                ):  # Larger chunk size for efficiency
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress.update(task, completed=downloaded)
                        else:
                            # For unknown size, just update the description periodically
                            progress.update(
                                task,
                                description=f"[cyan]Downloading {file_name} ({downloaded / 1024 / 1024:.2f} MB)",
                            )

            # Mark as complete and show final size
            final_size = os.path.getsize(full_path) / (1024 * 1024)  # Size in MB
            progress.update(
                task,
                description=f"[bold green]✓ {file_name} complete ({final_size:.2f} MB)",
            )

            if total_size == 0:  # If we didn't know the size before
                progress.update(task, completed=100, total=100)  # Mark as complete

            return full_path

        except Exception as e:
            progress.update(
                task, description=f"[bold red]Failed: {file_name} - {str(e)}"
            )
            console.print(f"[bold red]Error downloading {file_name}: {str(e)}")
            raise

    def extract(self):
        console.print("[bold blue]Extracting downloaded files...[/]")

        # Use StatusManager here too
        with StatusManager.status(
            "[bold cyan]Extracting clinical data...", spinner="dots"
        ) as status:
            tar_file = tarfile.open(self.clinical_tar_file)
            tar_file.extractall(self.temp_folder)
            tar_file.close()
            status.update("[bold green]Clinical data extracted successfully")

        with StatusManager.status(
            "[bold cyan]Extracting biospecimen data...", spinner="dots"
        ) as status:
            tar_file = tarfile.open(self.biospecimen_tar_file)
            tar_file.extractall(self.temp_folder)
            tar_file.close()
            status.update("[bold green]Biospecimen data extracted successfully")

        with StatusManager.status(
            "[bold yellow]Cleaning up archive files...", spinner="dots"
        ) as status:
            os.remove(self.clinical_tar_file)
            os.remove(self.biospecimen_tar_file)
            status.update("[bold green]Archive files removed")

        console.print(
            Panel(
                "[bold green]✓ All files extracted successfully",
                title="Extraction Complete",
                border_style="green",
            )
        )

    def get_temp_folder(self):
        return self.temp_folder

    def threaded_update(self):
        console.print(
            Panel(
                "[bold blue]Starting database update process",
                title="Update Process",
                border_style="blue",
            )
        )

        # Create temp folder if it doesn't exist
        os.makedirs(self.temp_folder, exist_ok=True)

        # Use a single progress display for all downloads
        console.print("[bold blue]Downloading data files...[/]")

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            refresh_per_second=10,  # More frequent updates
            console=console,
        ) as progress:
            clinical_task = progress.add_task(
                "[cyan]Clinical data", total=None
            )  # Start with unknown size
            biospecimen_task = progress.add_task(
                "[cyan]Biospecimen data", total=None
            )  # Start with unknown size

            with ThreadPoolExecutor(max_workers=2) as executor:
                clinical_future = executor.submit(
                    self.download_with_progress,
                    self.CLINICAL_URL,
                    self.clinical_tar_file,
                    progress,
                    clinical_task,
                )
                biospecimen_future = executor.submit(
                    self.download_with_progress,
                    self.BIOSPECIMEN_URL,
                    self.biospecimen_tar_file,
                    progress,
                    biospecimen_task,
                )

                # Wait for downloads to complete
                try:
                    clinical_future.result()
                    biospecimen_future.result()
                except Exception as e:
                    console.print(f"[bold red]Error during download: {str(e)}")
                    raise

        # After downloads complete, extract files
        self.extract()
        console.print("[bold green]Update process completed successfully!")
