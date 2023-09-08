from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv


class DatabaseManager:
    def __init__(self):
        load_dotenv()
        host = os.getenv("HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("PASSWORD")
        database = os.getenv("DATABASE")
        database_url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.engine = create_engine(database_url)

    def execute(self, query):
        return pd.read_sql(query, self.engine)

    def get_case_ids(self, query):
        df = self.execute(query)
        if "case_id" in df.columns:
            return df["case_id"].tolist()
        else:
            raise ValueError(
                "The result of the query does not contain 'case_id' column."
            )

    def get_cohort(self, query):
        cohort_case_ids = self.get_case_ids(query)
        unique_case_ids = set(cohort_case_ids)
        return list(unique_case_ids)
