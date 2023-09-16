from datetime import datetime
from typing import Union

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from util import create_snow_engine, read_write_file


def fetch_fixture(week: str) -> str:
    """
    Fetch the status (finished or not) of the last game for a given week from the Fantasy Premier League API.

    Parameters:
    - week (int): The game week for which to retrieve fixture status.

    Returns:
    - str or None: The status of the last game ('Finished' or 'Not Finished') or None if no data is available.

    """
    url = "https://fantasy.premierleague.com/api/fixtures/"

    reponse = requests.get(url, timeout=60)
    fixtures = reponse.json()
    data = pd.json_normalize(fixtures)
    last_game_status = data.loc[data["event"] == week, "finished"].iloc[-1]

    return last_game_status


def week_file(week: int = 4, file: str = "weeks.txt") -> float:
    """Read the last week number from a file or create the file with a default value if it doesn't exist."""
    try:
        file_content = read_write_file(file, "r")
        week = int(file_content[-1].strip())
    except:
        read_write_file(file, "w", f"{week} \n")

    return float(week)


def ge_result(response: requests.Response) -> pd.DataFrame:
    """Extract and normalize data from a JSON response and return it as a Pandas DataFrame."""

    df = pd.json_normalize(response.json()["standings"]["results"])

    return df


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 15),
    catchup=True,
    tags=["FPL"],
)
def standing_pipeline():
    """ """

    @task.short_circuit()
    def is_last_game_played() -> bool:
        """Check if the last game of the current week in the Fantasy Premier League has been played."""

        current_week = week_file()

        has_last_game_played = fetch_fixture(current_week)
        print(has_last_game_played)
        return has_last_game_played

    def fetch_standing(page: int) -> Union[requests.Response, None]:
        """
        Fetch YDP FPL League standing.

        Parameters:
        - page (int): The page number.
        """
        url = f"https://fantasy.premierleague.com/api/leagues-classic/885844/standings?page_standings={page}"

        response = requests.get(url, timeout=60)

        if response.status_code == 200:
            print("Request successful!")
            return response

        print(f"Request failed with status code: {response.status_code}")
        return

    @task
    def get_standing(resp: requests.Response) -> str:
        """
        Retrieve and concatenate multiple pages of standings data and return it as a JSON string.

        Returns:
        - str: A JSON containing the concatenated standings data.
        """
        try:
            standing_json = []
            start_page = 1
            has_next = True
            while has_next:
                print(f"{start_page} standing")
                response = fetch_standing(start_page)
                has_next = response.json()["standings"]["has_next"]
                standing_json.append(ge_result(response))

                start_page += 1

            standing_df_json = pd.concat(standing_json)
            print(standing_df_json)
            standing_df_json.reset_index(drop=True, inplace=True)
            return standing_df_json.to_json()
        except Exception as e:
            print(e)

    @task
    def load_to_warehouse(data: dict) -> bool:
        with create_snow_engine(uri) as engine:
            df = pd.read_json(data)
            print(df)
            df.to_sql("standing", if_exists="append", con=engine, index=False)
        return True

    @task
    def update_game_week(*args):
        file_content = read_write_file("weeks.txt", "r")
        week = int(file_content[-1].strip())
        new_week = int(week) + 1
        read_write_file("weeks.txt", "a", f"{new_week} \n")

    uri = Variable.get("db_uri")
    resp = is_last_game_played()
    parsed_data = get_standing(resp)
    status2 = load_to_warehouse(parsed_data)

    ugw = update_game_week(status2)

    status2 >> ugw

    managers_team_trigger = TriggerDagRunOperator(
        task_id="fetch_managers",
        trigger_dag_id="managers_team_pipeline",
        conf={"message": "Hello World"},
    )
    players_triggers = TriggerDagRunOperator(
        task_id="fetch_players",
        trigger_dag_id="players_pipeline",
        conf={"message": "Hello World"},
    )
    fixtures_triggers = TriggerDagRunOperator(
        task_id="fetch_fixtures",
        trigger_dag_id="fixtures_pipeline",
        conf={"message": "Hello World"},
    )
    players_stats_triggers = TriggerDagRunOperator(
        task_id="player_stats_start",
        trigger_dag_id="players_stat_pipeline",
        conf={"message": "Hello World"},
    )
    (
        ugw
        >> [players_triggers, fixtures_triggers]
        >> managers_team_trigger
        >> players_stats_triggers
    )


rss = standing_pipeline()
