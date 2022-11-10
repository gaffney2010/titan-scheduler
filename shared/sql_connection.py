"""Shared connection object that uses fixed titan server."""

import contextlib
import os

import MySQLdb
import titanpublic


@contextlib.contextmanager
def titan():
    host = titanpublic.shared_logic.get_secrets(
        os.path.dirname(os.path.abspath(__file__))
    )["aws_host"]
    port = 3306
    dbname = titanpublic.pod_helpers.database_resolver(
        os.environ.get("SPORT"), os.environ.get("TITAN_ENV", "dev")
    )
    user = titanpublic.shared_logic.get_secrets(
        os.path.dirname(os.path.abspath(__file__))
    )["aws_username"]
    password = titanpublic.shared_logic.get_secrets(
        os.path.dirname(os.path.abspath(__file__))
    )["aws_password"]

    connection = MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=dbname
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
