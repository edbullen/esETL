"""
Postgres Database Specific Functions
"""

from sqlalchemy import create_engine
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    import psycopg2
import psycopg2

import esextract
import numpy

class ValuesNumpyArrayTypeError(Exception):
    pass

def connection(username, password, host, port, database):
    """
    Connect to a Postgres Database and pass back a connection
    """

    ## Database Connect ##
    try:
        engine = create_engine(
            'postgresql+psycopg2://' + username + ':' + password + '@' + host + ':' + port + '/' + database)
        conn = engine.raw_connection()

    except Exception as e:
        esextract.log("Postgres Database Connect Error:")
        esextract.log(str(e))
        raise
    ########################

    return conn


def insert_statement(conn, insert_stmt, values_ndarray):
    """
    Insert to database
    :param cur: cursor
    :param insert_stmt: statement to execute
    :param values_ndarray: values to insert, passed in as numpy ndarry (i.e. Pandas df.values)
    :return:
    """
    complete = False

    if not isinstance(values_ndarray, numpy.ndarray ):
        esextract.log("invalid values array data-type passed")
        esextract.log(str(type(values_ndarray)))
        raise ValuesNumpyArrayTypeError

    try:
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, values_ndarray)
        conn.commit()
        esextract.log("   Commited")
        complete = True
    except Exception as e:
        esextract.log("Postgres Database Insert Error:")
        esextract.log(str(e))
        raise

    cur.close()
    conn.close()

    return complete

def select_statement(conn, select_stmt,logPrintFlag):
    """

    :param conn:
    :param select_stmt:
    :return: list-of-lists
    """

    try:
        cur = conn.cursor()
        esextract.log("    Running Query...", logPrintFlag)
        cur.execute(select_stmt)
        records = cur.fetchall()
    except Exception as e:
        esextract.log("Postgres Database Query Error:")
        esextract.log(str(e))
        raise

    cur.close()
    conn.close()

    esextract.log("    Complete", logPrintFlag)
    for row in records:
        result = row
    return(result)