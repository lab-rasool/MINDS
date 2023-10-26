from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv


class DatabaseManager:
    def __init__(self):
        os.environ.pop("HOST", None)
        os.environ.pop("DB_USER", None)
        os.environ.pop("PASSWORD", None)
        os.environ.pop("DATABASE", None)
        load_dotenv()
        host = os.getenv("HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("PASSWORD")
        database = os.getenv("DATABASE")
        database_url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.engine = create_engine(database_url)

    def execute(self, query):
        return pd.read_sql(query, self.engine)

    def get_cohort(self, query):
        df = self.execute(query)
        cohort = df.groupby("case_id")["case_submitter_id"].unique()
        return cohort
