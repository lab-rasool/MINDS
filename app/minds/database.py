import logging
import os
import shutil

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras


class DatabaseManager:
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
        self.connection = self.connect_to_db(self.config)

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
            logging.error(f"Error connecting to PostgreSQL database: {e}")
            return None

    def execute(self, query):
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query)
            return pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
        except psycopg2.Error as e:
            logging.error(f"Error executing query: {e}")
            raise

    def get_minds_cohort(self, query):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        df = self.execute(query)
        cohort = df.groupby("case_id")["case_submitter_id"].unique()
        return cohort

    def get_gdc_cohort(self, gdc_cohort):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cohort = pd.read_csv(gdc_cohort, sep="\t", dtype=str)
        query = f"""
            SELECT case_id, case_submitter_id 
            FROM clinical 
            WHERE case_id IN ({",".join([f"'{i}'" for i in cohort["id"]])})
        """
        df = self.execute(query)
        cohort = df.groupby("case_id")["case_submitter_id"].unique()
        return cohort

    def get_tables(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        tables_in_db = self.execute(query)
        tables = tables_in_db["table_name"]
        return tables

    def get_columns(self, table):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}'
        """
        columns = self.execute(query)
        return columns["column_name"]

    def update(self, temp_folder):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        logging.info("Uploading new data to the database")

        try:
            for file in os.listdir(temp_folder):
                table_name = file.split(".")[0]
                df = pd.read_csv(f"{temp_folder}/{file}", sep="\t", dtype=str)
                df.replace("'--", np.nan, inplace=True)

                if table_name in self.get_tables().tolist():
                    logging.info(f"Updating {table_name}")
                    with self.connection.cursor() as cursor:
                        for index, row in df.iterrows():
                            columns = ", ".join(row.index)
                            values = ", ".join(
                                [
                                    f"'{value}'" if pd.notna(value) else "NULL"
                                    for value in row
                                ]
                            )
                            query = f"""
                                INSERT INTO {table_name} ({columns}) 
                                VALUES ({values}) 
                                ON CONFLICT DO NOTHING
                            """
                            cursor.execute(query)
                else:
                    logging.info(f"Creating {table_name}")
                    with self.connection.cursor() as cursor:
                        columns = ", ".join(df.columns)
                        create_table_query = f"""
                            CREATE TABLE {table_name} ({", ".join([f"{col} TEXT" for col in df.columns])})
                        """
                        cursor.execute(create_table_query)
                        for index, row in df.iterrows():
                            values = ", ".join(
                                [
                                    f"'{value}'" if pd.notna(value) else "NULL"
                                    for value in row
                                ]
                            )
                            insert_query = f"""
                                INSERT INTO {table_name} ({columns}) 
                                VALUES ({values}) 
                                ON CONFLICT DO NOTHING
                            """
                            cursor.execute(insert_query)
            self.connection.commit()
            logging.info("Finished uploading to the database")
        except psycopg2.Error as e:
            self.connection.rollback()
            logging.error(f"Error during update: {e}")
            raise
        finally:
            self.connection.close()
            shutil.rmtree(temp_folder)
