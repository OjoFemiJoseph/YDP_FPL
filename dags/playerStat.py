from datetime import datetime
from typing import List, Union

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
def players_stat_pipeline():
    """ """

    @task
    def player_stats_start() -> None:
        """Retrieve player IDs from the database and save them to a CSV file."""
        with create_snow_engine(uri) as engine:
            df = pd.read_sql("select distinct id from players", engine)
            df.to_csv("players.csv", index=False)
        return

    def get_stat(row: pd.Series, base_url: str) -> Union[dict, None]:
        """Retrieve player statistics from an API using the player's ID"""
        retry = 0
        while retry < 3:
            player_id = row["id"]
            api_url = base_url.format(player_id)

            response = requests.get(api_url, timeout=60)
            if response.status_code == 200:
                player_stat = response.json()
                player_stat["player_id"] = player_id
                return player_stat
            else:
                print(f"Request failed for ID {player_id}")
                retry += 1

    @task
    def get_all_players_stat(*args) -> list:
        """
        Iterate through each player and get their FPL stats for all gameweek.
        """
        base_url = "https://fantasy.premierleague.com/api/element-summary/{}/"
        df = pd.read_csv("players.csv")
        results = []

        for idx, row in df.iterrows():
            player_stat = get_stat(row, base_url)
            results.append(player_stat)

        return results

    @task
    def parse_history(results: list) -> list:
        """Parse player history data from a list of results."""
        player_stat_df = []

        for player_result in results:
            history = player_result["history"]
            player_stats = pd.json_normalize(history)
            if not player_stats.empty:
                player_stat_df.append(player_stats)

        return player_stat_df

    @task
    def load_warehouse(player_stat_df: List[pd.DataFrame]) -> None:
        """Load player statistics data into the warehouse."""
        with create_snow_engine(uri) as engine:
            player_stat = pd.concat(player_stat_df)
            player_stat.to_sql(
                "Player_stats", if_exists="replace", con=engine, index=False
            )

    uri = Variable.get("db_uri")
    stats = player_stats_start()
    all_play_stats = get_all_players_stat(stats)
    history = parse_history(all_play_stats)
    load_warehouse(history)


rss = players_stat_pipeline()
