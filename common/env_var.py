import os

from airflow.models import Variable


def get(var_name: str) -> str:
    try:
        return Variable.get(var_name)
    except Exception:
        return os.environ[var_name]


def set(var_name: str, value: str):
    try:
        Variable.set(var_name, value)
    except Exception:
        os.environ[var_name] = value
