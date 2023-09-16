import json
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from util import create_snow_engine


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 9, 15),
    catchup=False,
    tags=["FPL"],
)
def fixtures_pipeline():
    """ """

    @task
    def fetch_fixtures() -> list:
        """
        Fetch fixtures data from the FPL API.

        Returns:
        - dict: JSON data containing fixture information.
        """

        url = "https://fantasy.premierleague.com/api/fixtures/"
        rq = requests.get(url, timeout=60)

        return rq.json()

    @task
    def load_fixtures(data: str) -> None:
        """
        Load fixtures data into the database.

        Parameters:
        - data (dict): JSON data containing fixture information.
        """

        with create_snow_engine(uri) as engine:
            df = pd.json_normalize(data)
            x = df.select_dtypes(include=["object"]).columns
            for i in x:
                df[i] = list(map(lambda x: json.dumps(x), df[i]))

            df.to_sql(
                "fixtures",
                con=engine,
                if_exists="replace",
                index=False,
            )

    uri = Variable.get("db_uri")
    resp = fetch_fixtures()
    load_fixtures(resp)


rss = fixtures_pipeline()
