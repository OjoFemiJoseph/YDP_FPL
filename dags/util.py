from contextlib import contextmanager
from typing import Union

import boto3
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


@contextmanager
def create_snow_engine(uri: str):
    """
    Create and return a SQLAlchemy database engine.

    Parameters:
    - uri (str): database connection URI.

    Yields:
    - engine: SQLAlchemy engine connected to the database.

    Raises:
    - Exception: If there's an error in creating the engine.
    """

    try:
        engine = create_engine(uri)
        yield engine
    except SQLAlchemyError as e:
        raise Exception(f"Failed to create db engine: {str(e)}")
    finally:
        engine.dispose()


def read_write_file(file_path: str, mode: str, data: Union[str, None] = None) -> str:
    """
    Utility function to read from or write to a file.

    Args:
        file_path (str): The path to the file.
        mode (str): The mode for file operations ('r' for read, 'w' for write, 'a' for append, etc.).
        data (str): The data to write to the file (only required for write/append modes).

    Returns:
        str: If in read mode, returns the content read from the file.
             If in write/append mode, returns 'Data written successfully' upon successful write.
    """
    try:
        if mode in ("r", "rb"):
            with open(file_path, mode) as file:
                content = file.readlines()
            return content
        elif mode in ("w", "wb", "a", "ab"):
            with open(file_path, mode) as file:
                file.write(data)
            return "Data written successfully"
        else:
            return 'Invalid mode. Use "r", "w", "a", etc.'
    except Exception as e:
        raise Exception(f"File operation failed: {str(e)}")
