"""Shared connection object that uses fixed titan server."""

import contextlib
import os

import mysql
import titanpublic


@contextlib.contextmanager
def titan():
    host = titanpublic.shared_logic.get_secrets(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
    )["aws_host"]
    port = 3306
    dbname = titanpublic.pod_helpers.database_resolver(
        os.environ.get("SPORT"), os.environ.get("TITAN_ENV", "dev")
    )
    user = titanpublic.shared_logic.get_secrets(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
    )["aws_username"]
    password = titanpublic.shared_logic.get_secrets(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
    )["aws_password"]

    connection = mysql.connector.connect(
        user="tjg",
        password="DuckPage418",
        host="localhost",
        database=dbname,
    )

    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        connection.close()
