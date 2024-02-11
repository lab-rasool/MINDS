from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv
import numpy as np
import logging
import shutil


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
        database = os.getenv("DATABASE")
        database_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(database_url)

    def execute(self, query):
        return pd.read_sql(query, self.engine)

    def get_cohort(self, query):
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
        # make sure the temp folder exists
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)
        # upload all the files to the database as a table
        logging.info("Uploading new data to the database")
        for file in os.listdir(temp_folder):
            table_name = file.split(".")[0]
            df = pd.read_csv(f"{temp_folder}/{file}", sep="\t", dtype=str)
            df.replace("'--", np.nan, inplace=True)
            # if table already exists, append the new data
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
                    name=table_name, con=self.engine, if_exists="replace", index=False
                )
        logging.info("Finished uploading to the database")
        shutil.rmtree(temp_folder)
