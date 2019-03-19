"""

ElasticSearch extract utility.

Load elasticsearch index host and port from config ini file

Extract - query some cols (based on a config file) and create a pandas data-frame to either dump to CSV or load to database

Extract a range of data between two vals based on a range-key and also filter by (another) key - value pair

"""

from elasticsearch import Elasticsearch
import pandas as pd
import os
import sys
import configparser
import time
import re
import datetime
import argparse


import pwdutil  # utility for retreiving  password that is not stored in clear-text fmt.  Requires previous setup and .key file configuration

import postgres_db # Postgres DB functions

CONFIG_PATH = os.getcwd() + "/conf/esextract.conf"
KEY_PATH = os.getcwd() + "/conf" # just specify the directory, not filename
LOG_PATH = os.getcwd() + "/log/esextract.log"
CSV_PATH = os.getcwd() + "/log/esextract.csv"

SCROLL_SIZE = 10000

class ConfigFileAccessError(Exception):
    pass

class ConfigFileParseError(Exception):
    pass

class DataFrameColsSpecification(Exception):
    pass

def fileexists(fname):
    return (os.path.isfile(fname))

def gettimestamp():
    return str(datetime.datetime.now())[0:19] + " "

def getconfig(CONFIG_PATH):
    """
    Read the config file and populate a dictionary of params with values.

    returns a dictionary of "sections" with each section having a dictionary of params
    sections{sectionA: {option: val option, val}, sectionB {option: val, option: val}

    """
    Config = configparser.ConfigParser()

    if fileexists(CONFIG_PATH):
        Config.read(CONFIG_PATH)
        # populate sections dict with params for each section
        sections = {}
        for section in Config.sections():
            params = {}

            options = Config.options(section)
            for option in options:
                try:
                    # params[option] = Config.get(section, option)
                    params[option] = Config.get(section, option)
                except:
                    raise ConfigFileParseError
            sections[section] = params
    else:
        raise ConfigFileAccessError(CONFIG_PATH)
    return sections


def log(text):
    print(gettimestamp(), text)
    text = gettimestamp() + " " + str(text) + "\n"
    with open(LOG_PATH, "a") as f:
        f.write(text)


def log_rotate():
    if os.path.exists(LOG_PATH):
        # Occasionally get spurious file permission / locking issues on windows
        # don't let this interrupt the data-load
        try:
            PREVIOUS_LOG = LOG_PATH + ".previous"
            if os.path.exists(PREVIOUS_LOG):
                os.remove(PREVIOUS_LOG)
            os.rename(LOG_PATH, PREVIOUS_LOG)
        except:
            pass


def date_to_epoc(year, month, day, hr24, min=0, sec=0):
    """
    Convenience fn to convert a date to EPOC / UNIX-TS fmt to feed into ES query by EPOC-fmt  time range
    :param year:
    :param month:
    :param day:
    :param hr24:
    :param min:
    :param sec:
    :return: integer representing seconds since UNIX EPOC
    """
    epoc_timestamp = round(datetime.datetime(year, month, day, hr24, min, sec).timestamp())
    return epoc_timestamp


def get_cols(cols_file):
    try:
        if cols_file:
            # Read in the columns to parse from the colsfile
            f = open(cols_file, 'r')
            cols = f.readlines()
            f.close()
            cols = [x.strip() for x in cols]
    except Exception as e:
        raise DataFrameColsSpecification(e)
    return cols


def extract_data_range(inputsource, filterkey, filterval, rangefield, startrange, endrange=None, cols_file=None,
                       csvfile=None, database_conf=None):
    """
    Query ElasticSearch for a given filter and range-field with startrange and endrange vars
    Null endrange means scan to end.
    Either dump to CSV or write to db during loop through of batches of results from ES
    :param inputsource - this is a config ref to the ES Host to read data from
    :param filterkey:
    :param filterval:
    :param rangefield:
    :param startrange:
    :param endrange:
    :param cols_file: file that contains list of cols to extract and load
    :param csvfile:  path to file
    :param database_conf:  Config Identifier for Database
    :return: number of records "n" processes
    """
    sections = getconfig(CONFIG_PATH)
    params = sections[inputsource]

    startrange = str(startrange)
    endrange = str(endrange)

    # Checks
    if database_conf is None and csvfile is None:
        raise AttributeError('must specify csvfile or database')
    if database_conf and csvfile:
        raise AttributeError('cannot specify csvfile AND database')
    if cols_file is None:
        raise AttributeError('No Cols configuration specified')

    # Query Elastic Search
    log("Extract Data between range " + startrange + " and " + endrange + " for " + rangefield)
    log(" for filterkey:" + filterkey + " filterval:" + filterval)
    log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport']}])

    # Final extracted list of results
    extract = []
    n = 0  # number of records processed
    for index_name in es.indices.get('*'):
        # scroll through results to handle > 10,000 records
        body_string = """{
                            "query": {
                                "bool": {
                                    "must": [{
                                    "match": { "<filterkey>": "<filterval>"}
                                    }
                                ,
                                    {
                                    "range": { "<rangefield>": {"gte": "<startrange>"
                                                           ,"lte": "<endrange>"
                                                          } 

                                             }
                                    }
                                    ]
                                }
                            }
                         }
                      """
        # replace the tags in the template ElasticSearch query with filter vals
        body_string = body_string.replace("<filterkey>", filterkey)
        body_string = body_string.replace("<filterval>", filterval)
        body_string = body_string.replace("<rangefield>", rangefield)
        body_string = body_string.replace("<startrange>", startrange)

        # if no end-range is specified, remove the "lte" part of range search - search to end
        if endrange == "None":
            log("No End-Range - scan to latest record")
            body_string = body_string.replace(",\"lte\": <endrange>", "")
        else:
            body_string = body_string.replace("<endrange>", endrange)

        ###log("DEBUG: dumping ES search-body string")
        ###log(body_string)

        page = es.search(index=index_name,
                         scroll='2m',
                         size=SCROLL_SIZE,
                         body=body_string)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        log("Index: " + index_name + " Total_Records:" + str(es.cat.indices(index_name).split()[6]) + " Hits:" + str(
            scroll_size))

        # Start scrolling
        while (scroll_size > 0):
            # Extract page data to extract list (append)
            for i in range(0, len(page['hits']['hits'])):
                payload = page['hits']['hits'][i]['_source']
                extract.append(payload)
                if (len(extract) % 10000) == 0:
                    print("#", end='')
            print("\n")
            log("Extracted " + str(len(extract)) + " records")

            # instead of appending to extract, write to database or CSV
            # create a Pandas Data frame
            if database_conf:
                # log("Inserting data to database table " + database)
                data = create_dataframe(extract, cols_file)
                dataframe_to_db(data, database_conf)
            elif csvfile:
                # log("Writing CSV data to " + csvfile)
                data = create_dataframe(extract, cols_file)
                write_csv(data, csvfile)
            else:
                raise Exception

            # reset the extract list
            n = n + len(extract)
            extract = []
            log("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
    log("Total Data Extract and Load: " + str(n) + " records")

    return n


def extract_data_agg(filterkey='jobStatus', filterval='JOB_FINISH2', monthshist='now-12M', aggkey='cpuTime',
                     aggtype='sum'):
    """
    Push down a sum-aggregate query to ElasticSearch
    Aggregate hard-coded to MONTHS
    Supply key-value filter, date-range (needs to be months or years) - EG "gte": "now-12M"
    Supply key to sum the value of - EG: "cpuTime"
    return
    ** This fn only aggregates a single field; compound elasticsearch aggregate is possible future enhancment
    ** Didn't bother paging results; assume aggregate won't hit 100,000 record limit

    :param filterkey:
    :param filterval:
    :param monthshist:
    :param aggkey:
    :param aggtype:
    :return: list of key-val list: [ 'YYYY-MM', <int> ]
    """
    params = getconfig(CONFIG_PATH)

    # Query Elastic Search
    log("Aggregate Data for last n months:" + monthshist + " filterkey:" + filterkey + " filterval:" + filterval)
    log("aggregating for " + aggkey)
    log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport']}])

    # Final extracted list of results
    extract = []
    results = es.search(index="*", body={"query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {"match": {"jobStatus": "JOB_FINISH2"}},
                        {"range": {"@timestamp": {"gte": "now-12M"}}}
                    ]
                }
            }
        }
    }
        ,
        "size": 0,
        "aggs": {
            "group_by_month": {
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "month"
                },
                "aggs": {
                    "aggValue": {aggtype: {"field": aggkey}}
                }
            }
        }
    })

    log("Returned " + str(len(results['aggregations']['group_by_month']['buckets'])) + " aggregations")

    for r in results['aggregations']['group_by_month']['buckets']:
        month = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['key'] / 1000))[:7]  # ES down-grades to EPOC
        val = round(r['aggValue']['value'], 2)
        row = [month, val]
        extract.append(row)

    return extract


def create_dataframe(extract, cols_file=None, cols=None):
    """
    Create a Pandas DataFrame from a data "extract" list-of-lists
    Either pass in a cols-file to open or a list of col-names
    :param extract:
    :param cols_file:
    :param cols:
    :return: Pandas data-frame w
    """
    log("   Building Pandas DataFrame")

    cols = get_cols(cols_file)

    dataframe = pd.DataFrame(extract, columns=cols)

    cols_list = re.sub(r' +', '', (str(dataframe.columns).replace(',', '\n').replace('\n\n', '\n')))
    cols_list = re.sub(r'\(\[', '\n', cols_list)
    cols_list = re.sub(r']\n', '', cols_list).replace("dtype='object')", "").replace("Index", "Columns:")

    log("   Dimensions: " + str(dataframe.shape))
    return dataframe


# def dataframe_to_db(data, table_name):
def dataframe_to_db(data, database_conf):
    """
    pass a dataframe in for a bulk-insert to database
    :param data:  Pandas data-frame
    :param database_conf: Config Identifier that maps to database, host, port, username, tablename
    :return: (None)
    """

    sections = getconfig(CONFIG_PATH)
    params = sections[database_conf]
    username = params["dbusername"]
    host = params["dbhost"]
    port = params["dbport"]
    database = params["database"]
    table_name = params["table"]

    # bespoke utility for retreiving obsfucated password from ./conf dir
    password = pwdutil.decode(pwdutil.get_key(KEY_PATH + "/.key_" + database_conf), pwdutil.get_pwd(pwdfile=KEY_PATH + "/.pwd_" + database_conf))

    log("   Insert to database - table " + table_name)
    log("   Rows:" + str(len(data)))

    # Database Connect
    if params["type"] == "postgres":
        conn = postgres_db.connection(username, password, host, port, database)
    else:
        log("Database Connect to " + params["type"] + " not supported")

    # Format data to insert
    df_columns = list(data)
    # Strip any "@' symbols - not supported in Postgres
    df_columns = [l.replace("@", "") for l in df_columns]
    # create (col1,col2,...)
    columns = ",".join(df_columns)

    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
    # print(insert_stmt)

    # Database Execute Insert an Numpy ndarray of Values = pandas df.values
    if params["type"] == "postgres":
        postgres_db.insert_statement(conn, insert_stmt, data.values)
    else:
        log("Database Connect to " + params["type"] + " not supported")

"""    
def maxval_from_db(filterkey, filterval, rangefield, table_name):
    """
    :param filterkey: the key to filter results on
    :param filterval: the value to filter by
    :param rangefield: the field to select range on - MAX
    :param table_name: the schema.table_name specifier
    :return: MAX val for rangefield - STRING value (we don't know what type this is)
    """
    maxval = ""

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cur, insert_stmt, data.values)
        conn.commit()
        log("   Commited")
    except psycopg2.Error as e:
        diag = e.diag
        log("Postgres Database Error:")
        log(str(e.pgerror))
        raise

    cur.close()
    conn.commit()

    return maxval
"""

def write_csv(data, filename):
    log("   Appending data to " + filename)
    data.to_csv(args["csvfile"], mode='a', header=False)


if __name__ == '__main__':
    params = getconfig(CONFIG_PATH)

    if sys.argv[1] == "dumpparams":
        for key, value in params.items():
            print(key)
            for sectionkey, sectionval in params[key].items():
                print("\t", sectionkey, ":", sectionval)

        exit(0)

    parser = argparse.ArgumentParser(description="""** ElasticSearch extract utility to dump to CSV or Load to Postgres Database. ** 

If loading to Postgres, data is read from ES to local memory then inserted to remote database
Connect configuration for ES and Postgres is configured in esextract.conf
Columns (fields) of data are extracted from ElasticSearch based on spec. in cols.conf
If writing to a database table, table-cols must match the structure of cols extracted from ElasticSearch 

EXAMPLE - extract last days record (default range on @timestamp) and load to Postgres database table "my_schema.my_table":
    $> python esextract.py -n 1 -k jobStatus -f JOB_FINISH -d my_schema.my_table
EXAMPLE - extract a range of values based on an integer value for the endTime field and dump to CSV:
    $> python esextract.py -r 1541680814#1542967602 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of values from a start-point to the highest value (unbounded end of range):
    $> python esextract.py -r 1541680814 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of timestamp values
    $> python esextract.py -i HPCElasticSearch -r 2019-01-31T14:02:39.000Z#2019-02-01T14:02:39.000Z -k jobStatus -f JOB_FINISH2 -c ../test.csv

"""
                                     , formatter_class=argparse.RawTextHelpFormatter)

    group_extract = parser.add_mutually_exclusive_group(required=True)
    group_extract.add_argument('-n', '--numdays', dest="numdays", help='Extract last n days')
    group_extract.add_argument('-r', '--range', dest="range", help='range extract <FROM>#<TO> - use # as separator')

    parser.add_argument('-i', '--input', dest="inputsource",
                        help='Input Source for Data - as defined in config file')
    parser.add_argument('-s', '--searchkey', dest="searchkey", default="@timestamp",
                        help='key for range or numdays to search on - i.e. last-n based on @timestamp / range on epoc val',
                        action='store')

    parser.add_argument('-k', '--key', dest="key", help='key for filtering on', action='store', required=True)
    parser.add_argument('-f', '--filter', dest="filter", help='value for filtering by', action='store', required=True)

    group_dest = parser.add_mutually_exclusive_group(required=True)
    group_dest.add_argument('-c', '--file', dest="csvfile", action='store'
                            , help='Save as CSV file')
    group_dest.add_argument('-d', '--database_config', dest="database_conf", action='store'
                            , help='database destination as defined in config file')
    # group.add_argument('-f', '--file', dest = "filename", default = PWD_FILE, help='Specify encoded filename', action='store')
    args = vars(parser.parse_args())

    if not args["inputsource"]:
        log("Input Source (-i) not specified - this must map to a configuration in the .conf file.  Exiting")
        exit(1)
    else:
        inputsource = args["inputsource"]

    log_rotate()  # Archive the old logfile
    cols_file = params[inputsource]["colsfile"]  # Cols spec to extract data for (and load cols spec for DB / csv)

    if args["csvfile"]:
        if fileexists(args["csvfile"]):
            log("CSV file already exists - exiting")
            exit(1)

    if args["numdays"]:
        print("Numdays set to", args["numdays"])
        extract = extract_data_days(filterkey=args["key"], filterval=args["filter"], rangefield=args["searchkey"],
                                    dayshist=args["numdays"])
        data = create_dataframe(extract, cols_file)
    elif args["range"]:
        print("Range set to", args["range"])

        #split range on "#" to hopefully avoid chars that appear in the key - else need a more sophisticated regex
        startrange = args["range"].split("#")[0]
        if len(args["range"].split("#")) == 2:
            endrange = args["range"].split("#")[1]

        n = extract_data_range(  inputsource=inputsource
                               , filterkey=args["key"]
                               , filterval=args["filter"]
                               , rangefield=args["searchkey"]
                               , startrange=startrange
                               , endrange=endrange
                               , cols_file=cols_file
                               , csvfile=args["csvfile"]
                               , database_conf=args["database_conf"]
                               )

    else:
        print("Unhandled mode - can only extract based on -n <numdays> or -r <startrange[:endrange]>")
        exit(1)
