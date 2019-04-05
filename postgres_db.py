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

    try:
        engine = create_engine(
            'postgresql+psycopg2://' + username + ':' + password + '@' + host + ':' + port + '/' + database)
        conn = engine.raw_connection()

    except Exception as e:
        esextract.log("Postgres Database Connect Error:")
        esextract.log(str(e))
        raise

    return conn


def insert_statement(conn, insert_stmt, values_ndarray, slice_start=None, slice_end=None):
    """
    Insert to database
    :param cur: cursor
    :param insert_stmt: statement to execute
    :param values_ndarray: values to insert, passed in as numpy ndarry (i.e. Pandas df.values)
    :return:
    """
    complete = False

    if slice_start:
        progress_message = str(slice_start) + ":" + str(slice_end)
    else:
        progress_message = ""

    if not isinstance(values_ndarray, numpy.ndarray ):
        esextract.log("invalid values array data-type passed")
        esextract.log(str(type(values_ndarray)))
        raise ValuesNumpyArrayTypeError

    try:
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, values_ndarray)
        conn.commit()
        esextract.log("   Commited " + progress_message)
        complete = True
    except Exception as e:
        esextract.log("Postgres Database Insert Error:")
        esextract.log(str(e))
        raise
    cur.close()


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

    esextract.log("    Complete", logPrintFlag)
    for row in records:
        result = row
    cur.close()
    return(result)

def insert_merge_statement(conn, merge_stmt, logPrintFlag):
    """
    Simple merge operation.  JUst insert from source into target table for all rows not in target
    :param conn:
    :param merge_stmt:
    :param logPrintFlag:
    :return: rowcount
    """
    rowcount = 0
    try:
        cur = conn.cursor()
        esextract.log("    Running Insert-Merge...", logPrintFlag)
        esextract.log("    " + merge_stmt, logPrintFlag)
        cur.execute(merge_stmt)
        rowcount = cur.rowcount
        conn.commit()
    except Exception as e:
        esextract.log("Postgres Database Error:")
        esextract.log(str(e))
        raise
    cur.close()
    return rowcount

def delete_statement(conn, delete_stmt, logPrintFlag):
    rowcount = 0
    try:
        cur = conn.cursor()
        #esextract.log("    Executing Delete", logPrintFlag)
        esextract.log("    " + delete_stmt, logPrintFlag)
        cur.execute(delete_stmt)
        rowcount = cur.rowcount
        conn.commit()
    except Exception as e:
        esextract.log("Postgres Database Error:")
        esextract.log(str(e))
        raise
    cur.close()
    return rowcount