import logging
import os
import shutil

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker


class DatabaseManager:
    def __init__(self, dotenv_path=".env"):
        os.environ.pop("HOST", None)
        os.environ.pop("PORT", None)
        os.environ.pop("DB_USER", None)
        os.environ.pop("PASSWORD", None)
        os.environ.pop("DATABASE", None)
        load_dotenv(dotenv_path=dotenv_path)
        host = os.getenv("HOST")
        port = os.getenv("PORT")
        user = os.getenv("DB_USER")
        password = os.getenv("PASSWORD")
        self.database = os.getenv("DATABASE")
        database_url = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{self.database}"
        )
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)

    def execute(self, query):
        try:
            with self.engine.connect() as connection:
                return pd.read_sql(query, connection)
        except SQLAlchemyError as e:
            logging.error(f"Error executing query: {e}")
            raise

    def get_minds_cohort(self, query):
        df = self.execute(query)
        cohort = df.groupby("case_id")["case_submitter_id"].unique()
        return cohort

    def get_gdc_cohort(self, gdc_cohort):
        cohort = pd.read_csv(gdc_cohort, sep="\t", dtype=str)
        query = f"""
            SELECT case_id, case_submitter_id 
            FROM {self.database}.clinical 
            WHERE case_id IN ({','.join([f"'{i}'" for i in cohort['id']])})
        """
        df = self.execute(query)
        cohort = df.groupby("case_id")["case_submitter_id"].unique()
        return cohort

    def get_tables(self):
        query = "SHOW TABLES"
        tables_in_db = self.execute(query)
        name_of_table = tables_in_db.columns[0]
        tables = tables_in_db[name_of_table]
        return tables

    def get_columns(self, table):
        query = f"SHOW COLUMNS FROM {table}"
        columns = self.execute(query)
        return columns["Field"]

    def update(self, temp_folder):
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        logging.info("Uploading new data to the database")

        session = self.Session()
        try:
            for file in os.listdir(temp_folder):
                table_name = file.split(".")[0]
                df = pd.read_csv(f"{temp_folder}/{file}", sep="\t", dtype=str)
                df.replace("'--", np.nan, inplace=True)

                if table_name in self.get_tables().tolist():
                    logging.info(f"Updating {table_name}")
                    df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists="append",
                        index=False,
                        chunksize=1000,
                    )
                else:
                    logging.info(f"Creating {table_name}")
                    df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists="replace",
                        index=False,
                    )
            session.commit()
            logging.info("Finished uploading to the database")
        except (SQLAlchemyError, PendingRollbackError) as e:
            session.rollback()
            logging.error(f"Error during update: {e}")
            raise
        finally:
            session.close()
            shutil.rmtree(temp_folder)
