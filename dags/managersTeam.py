from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from util import create_snow_engine, read_write_file


def get_gameweek(file: str = "weeks.txt") -> int:
    """
    Read the last game week from a file.

    Parameters:
    - file_path (str): Path to the file containing the game week.

    Returns:
    - int: The game week value.
    """
    try:
        file_content = read_write_file(file, "r")
        latest_week_str = file_content[-1].strip()
        if latest_week_str.isdigit() and int(latest_week_str) >= 0:
            latest_week = int(latest_week_str)
            previous_week = latest_week - 1
            return previous_week
        else:
            raise ValueError("Invalid data in file.")
    except Exception as e:
        raise Exception("File not found: ", e)


def get_manager_team(api_url: str, manager_id: str) -> dict:
    """Fetch manager's team data from an API and return it."""

    try:
        response = requests.get(api_url, timeout=60)

        if response.status_code == 200:
            manager_team = response.json()
            manager_team["manager_id"] = manager_id
            return manager_team
        else:
            print(f"Request failed for ID {manager_id}")
            return None
    except Exception as e:
        print(f"Request failed for ID {manager_id} with error: {str(e)}")
        return None


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 9, 15),
    catchup=False,
    tags=["FPL"],
)
def managers_team_pipeline():
    """ """

    @task
    def fetch_managers() -> pd.DataFrame:
        """
        Fetch distinct managers id from the db and saves it locally.

        Returns:
        - df (pd.DataFrame): A DataFrame containing the distinct Manager's id.
        """
        with create_snow_engine(uri) as engine:
            try:
                df = pd.read_sql("select distinct ENTRY from standing", engine)
                df.to_csv("managers.csv", index=False)
            except Exception as e:
                raise Exception(f"Failed to fetch managers: {str(e)}")

        return df

    @task
    def fetch_gameweek_stat(*args) -> list:
        """
        Fetch statistics for managers for the latest game week.

        Returns:
        - list: A list of dictionaries containing managers stats.
        """
        last_week = get_gameweek()
        df = pd.read_csv("managers.csv")

        managers_id = set(df[df.columns[0]].tolist())

        results = []

        for idx in managers_id:
            api_url = f"https://fantasy.premierleague.com/api/entry/{idx}/event/{last_week}/picks/"

            retry = 0
            while retry < 3:
                try:
                    Managers_team = get_manager_team(api_url, idx)
                    if Managers_team:
                        results.append(Managers_team)
                        retry = 5
                except Exception as e:
                    retry += 1
                    print(e)
        return results

    @task
    def load_warehouse(results: list) -> None:
        with create_snow_engine(uri) as engine:
            YDP = pd.json_normalize(
                results,
                record_path="picks",
                meta=[
                    "active_chip",
                    ["entry_history", "event"],
                    ["entry_history", "points_on_bench"],
                    "manager_id",
                ],
                errors="ignore",
            )
            YDP.columns = [
                "Players_id",
                "Player_FPL_position",
                "Multiplier",
                "Captain",
                "Vice_captain",
                "Active_chip",
                "Gameweek",
                "Bench_points",
                "Manager_id",
            ]
            YDP.to_sql("Managers_Team", if_exists="append", con=engine, index=False)

    uri = Variable.get("db_uri")
    managers = fetch_managers()
    stats = fetch_gameweek_stat(managers)
    load_warehouse(stats)


rss = managers_team_pipeline()
