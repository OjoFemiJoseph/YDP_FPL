# Fantasy Premier League (FPL) Data Pipeline with Apache Airflow

This repository contains an Apache Airflow DAG (Directed Acyclic Graph) for creating a data pipeline to collect and process Fantasy Premier League (FPL) data.

## Prerequisites

Before running the FPL pipeline, make sure you have the following installed and configured:

1. [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/): Used for managing containerized services.

2. Python 3.x: Required for writing custom operators and managing Airflow.

## Setup

1. **Clone this repository:**

    ```bash
    git clone https://github.com/OjoFemiJoseph/YDP_FPL.git
    cd YDP_FPL
    ```

2. **Start the Docker Compose services:**

    ```bash
    docker-compose up -d
    ```

    This will start Airflow and all the required services defined in `docker-compose.yml`.

3. **Access the Airflow web UI:**

    Open a web browser and go to [http://localhost:8085](http://localhost:8085). Log in to the Airflow UI using the default credentials (username: airflow, password: airflow).

4. **Set DB Variable:**

    - In the navbar, click on "Admin."
    - Then, click on "Variables."
    - Click the "+" button to create a variable, name it `db_uri`, and put in your database connection string.

5. **Trigger the Standing Pipeline:**

    Once you have configured the DAG and set the necessary variables, you can trigger the standing pipeline manually from the Airflow web UI .

Feel free to customize the pipeline and configurations as per your specific FPL data processing needs.
