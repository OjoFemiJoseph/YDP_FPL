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
def players_pipeline():
    """ """

    @task
    def fetch_players() -> dict:
        """
        Fetch players data from the Fantasy Premier League API.

        Returns:
        - dict: JSON data containing information about players.
        """
        url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        rq = requests.get(url, timeout=60)
        return rq.json()

    @task
    def load_players(data: dict) -> None:
        """
        Load players data into the database.

        Parameters:
        - data (dict): JSON data containing players information.
        """

        with create_snow_engine(uri) as engine:
            df = pd.json_normalize(data)
            players = pd.json_normalize(df["elements"].iloc[0])
            players.to_sql("players", con=engine, if_exists="replace", index=False)

    uri = Variable.get("db_uri")
    resp = fetch_players()
    load_players(resp)


rss = players_pipeline()
