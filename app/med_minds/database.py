import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.status import Status

console = Console()


class StatusManager:
    """Manager for Rich status displays to prevent multiple live displays error"""

    _active = False

    @classmethod
    def status(cls, message, spinner="dots"):
        """Create a status display with proper checking to prevent conflicts"""
        # Return a dummy context manager if a status is already active
        if cls._active:
            return DummyStatus(message)

        status = Status(message, spinner=spinner, console=console)
        cls._active = True

        # Create a wrapper to handle the status properly
        class StatusWrapper:
            def __enter__(self):
                status.__enter__()
                return status

            def __exit__(self, exc_type, exc_val, exc_tb):
                StatusManager._active = False
                return status.__exit__(exc_type, exc_val, exc_tb)

        return StatusWrapper()


class DummyStatus:
    """A dummy status that does nothing to prevent errors when nested"""

    def __init__(self, message):
        self.message = message

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def update(self, message):
        pass


class DatabaseManager:
    """
    DatabaseManager: A class for managing PostgreSQL database connections and operations.
    This class handles database connectivity, query execution, and data operations for the MINDS application.
    It provides methods to query data, retrieve database schema information, and update the database with
    new information from external files.
    Parameters
    ----------
    dotenv_path : str, optional
        Path to the .env file containing database credentials (default is ".env")
    Attributes
    ----------
    config : dict
        Database connection configuration parameters
    connection : psycopg2.extensions.connection
        Active connection to the PostgreSQL database
    Methods
    -------
    connect_to_db(config)
        Establishes connection to PostgreSQL database using provided configuration
    execute(query, use_status=True)
        Executes SQL query and returns results as a pandas DataFrame
    get_minds_cohort(query, use_status=True)
        Retrieves and processes MINDS cohort data using provided query
    get_gdc_cohort(gdc_cohort, use_status=True)
        Retrieves and processes GDC cohort data from a file
    get_tables(use_status=True)
        Returns list of tables in the connected database
    get_columns(table, use_status=True)
        Returns column names for a specified table
    update(temp_folder)
        Updates database with data from CSV/TSV files in the specified folder
    Notes
    -----
    - The class uses environment variables for database configuration
    - Requires pandas and psycopg2 packages
    - Visual progress reporting through rich library's console and progress features
    - Automatically cleans up temporary files after database updates
    """

    def __init__(self, dotenv_path=".env"):
        os.environ.pop("HOST", None)
        os.environ.pop("PORT", None)
        os.environ.pop("DB_USER", None)
        os.environ.pop("PASSWORD", None)
        os.environ.pop("DATABASE", None)
        load_dotenv(dotenv_path=dotenv_path)
        self.config = {
            "host": os.getenv("HOST"),
            "port": os.getenv("PORT"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("PASSWORD"),
            "database": os.getenv("DATABASE"),
        }
        with StatusManager.status("[bold blue]Connecting to database...") as status:
            self.connection = self.connect_to_db(self.config)
            if self.connection:
                status.update("[bold green]Database connection established")
            else:
                status.update("[bold red]Database connection failed")

    def connect_to_db(self, config):
        """Connect to PostgreSQL database."""
        try:
            connection = psycopg2.connect(
                host=config["host"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
                port=config.get("port", 5432),  # PostgreSQL default port
            )
            return connection
        except psycopg2.Error as e:
            console.print(f"[bold red]Error connecting to PostgreSQL database:[/] {e}")
            return None

    def execute(self, query, use_status=True):
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            if use_status:
                with StatusManager.status("[bold blue]Executing query...") as status:
                    cursor.execute(query)
                    status.update("[bold green]Query executed successfully")
            else:
                cursor.execute(query)
            return pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
        except psycopg2.Error as e:
            console.print(f"[bold red]Error executing query:[/] {e}")
            raise

    def get_minds_cohort(self, query, use_status=True):
        if use_status:
            with StatusManager.status(
                "[bold blue]Retrieving MINDS cohort..."
            ) as status:
                df = self.execute(query, use_status=False)
                cohort = df.groupby("cases_case_id")["cases_submitter_id"].unique()
                status.update(f"[bold green]Retrieved {len(cohort)} cohort entries")
        else:
            df = self.execute(query, use_status=False)
            cohort = df.groupby("cases_case_id")["cases_submitter_id"].unique()
        return cohort

    def get_gdc_cohort(self, gdc_cohort, use_status=True):
        if use_status:
            with StatusManager.status("[bold blue]Retrieving GDC cohort...") as status:
                cohort = pd.read_csv(gdc_cohort, sep="\t", dtype=str)
                query = f"""
                    SELECT cases_case_id, cases_submitter_id 
                    FROM clinical 
                    WHERE cases_case_id IN ({",".join([f"'{i}'" for i in cohort["id"]])})
                """
                df = self.execute(query, use_status=False)
                cohort = df.groupby("cases_case_id")["cases_submitter_id"].unique()
                status.update(f"[bold green]Retrieved {len(cohort)} cohort entries")
        else:
            cohort = pd.read_csv(gdc_cohort, sep="\t", dtype=str)
            query = f"""
                SELECT cases_case_id, cases_submitter_id 
                FROM clinical 
                WHERE cases_case_id IN ({",".join([f"'{i}'" for i in cohort["id"]])})
            """
            df = self.execute(query, use_status=False)
            cohort = df.groupby("cases_case_id")["cases_submitter_id"].unique()
        return cohort

    def get_tables(self, use_status=True):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        if use_status:
            with StatusManager.status(
                "[bold blue]Retrieving database tables..."
            ) as status:
                tables_in_db = self.execute(query, use_status=False)
                tables = tables_in_db["table_name"]
                status.update(f"[bold green]Retrieved {len(tables)} database tables")
        else:
            tables_in_db = self.execute(query, use_status=False)
            tables = tables_in_db["table_name"]
        return tables

    def get_columns(self, table, use_status=True):
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}'
        """
        if use_status:
            with StatusManager.status(
                f"[bold blue]Retrieving columns for table '{table}'..."
            ) as status:
                columns = self.execute(query, use_status=False)
                status.update(f"[bold green]Retrieved {len(columns)} columns")
        else:
            columns = self.execute(query, use_status=False)
        return columns["column_name"]

    def update(self, temp_folder):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        console.print("[bold blue]Uploading new data to the database")

        def sql_escape_value(value):
            """Helper function to properly format SQL values"""
            if pd.isna(value):
                return "NULL"
            else:
                # Split the string operations to avoid backslashes in f-strings
                value_str = str(value)
                escaped_value = value_str.replace("'", "''")
                return f"'{escaped_value}'"

        try:
            # Get file list first for progress tracking
            files = os.listdir(temp_folder)

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                overall_task = progress.add_task(
                    "[yellow]Processing files...", total=len(files)
                )

                for file in files:
                    table_name = file.split(".")[0]
                    file_task = progress.add_task(f"[cyan]Processing {file}", total=100)

                    # Update progress at key steps
                    progress.update(
                        file_task, advance=10, description=f"[cyan]Reading {file}"
                    )
                    df = pd.read_csv(f"{temp_folder}/{file}", sep="\t", dtype=str)
                    df.replace("'--", np.nan, inplace=True)

                    # Sanitize column names
                    df.columns = [
                        col.replace(".", "_").replace(" ", "_").replace("-", "_")
                        for col in df.columns
                    ]
                    progress.update(file_task, advance=20)

                    # Get tables without using StatusManager
                    existing_tables = self.get_tables(use_status=False).tolist()
                    progress.update(file_task, advance=10)

                    row_count = len(df)

                    if table_name in existing_tables:
                        progress.update(
                            file_task,
                            description=f"[cyan]Updating {table_name} ({row_count} rows)",
                        )
                        row_task = progress.add_task(
                            f"[green]Inserting rows into {table_name}", total=row_count
                        )

                        with self.connection.cursor() as cursor:
                            for index, row in df.iterrows():
                                columns = ", ".join(row.index)
                                values = ", ".join(
                                    [sql_escape_value(value) for value in row]
                                )
                                query = f"""
                                    INSERT INTO {table_name} ({columns}) 
                                    VALUES ({values}) 
                                    ON CONFLICT DO NOTHING
                                """
                                cursor.execute(query)
                                progress.update(row_task, advance=1)
                    else:
                        progress.update(
                            file_task,
                            description=f"[cyan]Creating {table_name} ({row_count} rows)",
                        )
                        row_task = progress.add_task(
                            f"[green]Creating and populating {table_name}",
                            total=row_count + 1,
                        )

                        with self.connection.cursor() as cursor:
                            columns = ", ".join(df.columns)
                            create_table_query = f"""
                                CREATE TABLE {table_name} ({", ".join([f'"{col}" TEXT' for col in df.columns])})
                            """
                            cursor.execute(create_table_query)
                            progress.update(
                                row_task, advance=1
                            )  # Count schema creation as one step

                            for index, row in df.iterrows():
                                values = ", ".join(
                                    [sql_escape_value(value) for value in row]
                                )
                                insert_query = f"""
                                    INSERT INTO {table_name} ({columns}) 
                                    VALUES ({values}) 
                                    ON CONFLICT DO NOTHING
                                """
                                cursor.execute(insert_query)
                                progress.update(row_task, advance=1)

                    # Mark file as complete
                    progress.update(
                        file_task,
                        completed=True,
                        description=f"[bold green]Completed {file}",
                    )
                    progress.update(overall_task, advance=1)

            self.connection.commit()
            console.print(
                Panel(
                    "[bold green]✓ Database update completed successfully!",
                    title="Success",
                    border_style="green",
                )
            )

        except Exception as e:
            self.connection.rollback()
            console.print(
                Panel(
                    f"[bold red]Error during update: {e}",
                    title="Database Update Failed",
                    border_style="red",
                )
            )
            raise
        finally:
            with StatusManager.status(
                "[bold yellow]Cleaning up temporary files..."
            ) as status:
                try:
                    # Safer directory cleanup
                    for root, dirs, files in os.walk(temp_folder, topdown=False):
                        for file in files:
                            try:
                                os.remove(os.path.join(root, file))
                            except Exception as e:
                                console.print(
                                    f"[yellow]Failed to remove file {file}: {e}"
                                )
                        for dir in dirs:
                            try:
                                os.rmdir(os.path.join(root, dir))
                            except Exception as e:
                                console.print(
                                    f"[yellow]Failed to remove directory {dir}: {e}"
                                )
                    if os.path.exists(temp_folder):
                        try:
                            os.rmdir(temp_folder)
                            status.update(
                                "[bold green]Temporary files cleaned up successfully"
                            )
                        except Exception as e:
                            status.update(
                                f"[bold yellow]Partially cleaned up - some files remain: {e}"
                            )
                except Exception as e:
                    status.update(f"[bold red]Error during cleanup: {e}")
