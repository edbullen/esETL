"""

ElasticSearch extract utility.

Load elasticsearch index host and port from config ini file

Extract - query some cols (based on a config file) and create a pandas data-frame

Extract by "last n days" - extract_data_days
 or range between two vals - extract_data_range

Filter on a given key-val.


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

from sqlalchemy import create_engine
import psycopg2

import pwdutil # simple utility for retreiving that is not stored in clear-text fmt.  Requires previous setup and
               # .key file configuration



CONFIG_PATH = os.getcwd() + "/conf/esextract.conf"
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
    return(os.path.isfile(fname) )

def gettimestamp():
    return str(datetime.datetime.now())[0:19] + " "

def getconfig(CONFIG_PATH):
    """
    Read the config file and populate a list of params with values.
    """
    Config = configparser.ConfigParser()
    params = {}

    if fileexists(CONFIG_PATH):
        Config.read(CONFIG_PATH)
        #only read [ElasticSearch] and [PostgeSQL] header in config file
        for section in Config.sections():
            if section == "ElasticSearch":
                options = Config.options(section)
                for option in options:
                    try:
                        params[option] = Config.get(section, option)
                    except:
                        raise ConfigFileParseError
            if section == "PostgreSQL":
                options = Config.options(section)
                for option in options:
                    try:
                        params[option] = Config.get(section, option)
                    except:
                        raise ConfigFileParseError

    else:
        raise ConfigFileAccessError(CONFIG_PATH)
    return params

def log(text):
    print(gettimestamp(), text)
    text = gettimestamp() + " " + str(text) + "\n"
    with open(LOG_PATH, "a") as f:
        f.write(text)

def log_rotate():
    if os.path.exists(LOG_PATH):
        #Occasionally get spurious file permission / locking issues on windows
        # don't let this interrupt the data-load
        try:
            PREVIOUS_LOG = LOG_PATH + ".previous"
            if os.path.exists(PREVIOUS_LOG):
                os.remove(PREVIOUS_LOG)
            os.rename(LOG_PATH, PREVIOUS_LOG)
        except:
            pass


def date_to_epoc(year, month,day,hr24,min = 0,sec = 0):
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
    epoc_timestamp = round(datetime.datetime(year,month,day,hr24,min,sec).timestamp())
    return epoc_timestamp


def extract_data_days(filterkey, filterval, rangefield, dayshist):
    """
    Data for last n days filtered by ElasticSearch key-value
    Pass a key to filter on and a value to filter for.
    ** Currently have to search through all indexes looking for a "hit" - indices only store create_time, not last update
    ** Planned enhancement - instead of returning the extract, either dump to CSV or write to db during loop (less mem overhead)
    Also pass integer "n" days to search forward from
    user ElasticSearch syntax for date specication

    :param filterkey:
    :param filterval:
    :param rangefield:
    :param dayshist:
    :return: extracted list of data records (list-of-lists)
    """

    dayshist_string = "now-" + str(dayshist) + "d"   # generate a string for ElasticSearch like "now-7d"
    params = getconfig(CONFIG_PATH)

    # Query Elastic Search
    log("Extract Data for last n Days: " + str(dayshist) + " filterkey:" + filterkey + " filterval:" + filterval)
    log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport'] }])

    #Final extracted list of results
    extract = []
    for index_name in es.indices.get('*'):
        # scroll through results to handle > 10,000 records
        # helpful example here: https://gist.github.com/drorata/146ce50807d16fd4a6aa

        page = es.search(index=index_name,
                            scroll = '2m',
                            size=SCROLL_SIZE,
                            body={
                                    "query": {
                                        "bool": {
                                            "must": [{
                                            "match": {filterkey:filterval}
                                            }
                                                ,
                                            {
                                            "range": { "@timestamp": {"gte": dayshist_string} }
                                            }]
                                        }
                                    }
                                 }
                            )

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        log("Index: " + index_name + " Total_Records:" + str(es.cat.indices(index_name).split()[6]) + " Hits:" + str(scroll_size))

        # Start scrolling
        while (scroll_size > 0):
            # Extract page data to extract list (append)
            log("Getting data...")
            for i in range(0, len(page['hits']['hits'])):
                payload = page['hits']['hits'][i]['_source']
                extract.append(payload)
            log("Extracted " + str(len(extract)) + " records")

            log("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
    log("Total Data Extract: " + str(len(extract)) + " records" )

    return extract


def extract_data_range(filterkey, filterval , rangefield, startrange, endrange=None ):
    """
    Query ElasticSearch for a given filter and range-field with startrange and endrange vars
    Null endrange means scan to end.
    ** Planned enhancement - instead of returning the extract, either dump to CSV or write to db during loop (less mem overhead)
    :param filterkey:
    :param filterval:
    :param rangefield:
    :param startrange:
    :param endrange:
    :return: extracted list of data records (list-of-lists)
    """
    params = getconfig(CONFIG_PATH)

    startrange = str(startrange)
    endrange = str(endrange)

    # Query Elastic Search
    log("Extract Data between range " + startrange + " and " + endrange + " for " + rangefield)
    log(" for filterkey:" + filterkey + " filterval:" + filterval)
    log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport'] }])

    #Final extracted list of results
    extract = []
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
                                                    "range": { "<rangefield>": {"gte": <startrange>
                                                                           ,"lte": <endrange>
                                                                          } 

                                                             }
                                                    }]
                                                }
                                            }
                                        }
                                        """
        #replace the tags in the template ElasticSearch query with filter vals
        body_string = body_string.replace("<filterkey>", filterkey)
        body_string = body_string.replace("<filterval>", filterval)
        body_string = body_string.replace("<rangefield>", rangefield)
        body_string = body_string.replace("<startrange>", startrange)

        #if no end-range is specified, remove the "lte" part of range search - search to end
        if endrange == "None":
            log("No End-Range - scan to latest record")
            body_string = body_string.replace(",\"lte\": <endrange>","")
        else:
            body_string = body_string.replace("<endrange>", endrange)

        ##DEBUG
        ##log(body_string)

        page = es.search(index=index_name,
                            scroll = '2m',
                            size=SCROLL_SIZE,
                            body=body_string)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        log("Index: " + index_name + " Total_Records:" + str(es.cat.indices(index_name).split()[6]) + " Hits:" + str(scroll_size))

        # Start scrolling
        while (scroll_size > 0):
            # Extract page data to extract list (append)
            for i in range(0, len(page['hits']['hits'])):
                payload = page['hits']['hits'][i]['_source']
                extract.append(payload)
                if (len(extract) % 1000) == 0:
                    print("#", end='')
            log("Extracted " + str(len(extract)) + " records")

            log("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
    log("Total Data Extract: " + str(len(extract)) + " records" )

    return extract


def extract_data_agg(filterkey = 'jobStatus', filterval = 'JOB_FINISH2', monthshist = 'now-12M', aggkey = 'cpuTime', aggtype = 'sum'):
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
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport'] }])

    #Final extracted list of results
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

    log("Returned " + str(len(results['aggregations']['group_by_month']['buckets'])) + " aggregations" )

    for r in results['aggregations']['group_by_month']['buckets']:
        month = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['key'] / 1000))[:7]  # ES down-grades to EPOC
        val = round(r['aggValue']['value'], 2)
        row = [month,val]
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
    log("Building Pandas DataFrame")
    try:
        if cols_file:
            # Read in the columns to parse from the colsfile
            f = open(cols_file, 'r')
            cols = f.readlines()
            f.close()
            cols = [x.strip() for x in cols]
        else:
            cols = cols
    except Exception as e:
        raise DataFrameColsSpecification(e)

    dataframe = pd.DataFrame(extract, columns=cols)

    cols_list = re.sub( r' +', '' , (str(dataframe.columns).replace(',','\n').replace('\n\n','\n') )   )
    cols_list = re.sub( r'\(\[', '\n', cols_list)
    cols_list = re.sub( r']\n', '', cols_list).replace("dtype='object')", "").replace("Index", "Columns:")
    log(cols_list)

    log("Dimensions: " + str(dataframe.shape))
    return dataframe

def dataframe_to_db(data, table_name):
    """
    pass a dataframe in an bulk-insert to Postgres DB
    :param data:
    :param table_name:
    :return: (None)
    """

    params = getconfig(CONFIG_PATH)
    username = params["dbusername"]
    host = params["dbhost"]
    port = params["dbport"]
    database = params["database"]

    #bespoke utility for retreiving obsfucated password from ./conf dir
    password=pwdutil.decode(pwdutil.get_key(), pwdutil.get_pwd(pwdfile= pwdutil.CONFIG_LOC + ".pwd"))

    log("insert to database - table " + table_name)
    log("rows:" + str(len(data)))
    engine = create_engine(
        'postgresql+psycopg2://' + username + ':' + password + '@' + host + ':' + port + '/' + database)

    conn = engine.raw_connection()
    cur = conn.cursor()
    df_columns = list(data)
    #Strip any "@' symbols - not supported in Postgres
    df_columns = [l.replace("@", "") for l in df_columns]

    # create (col1,col2,...)
    columns = ",".join(df_columns)

    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

    # create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
    #print(insert_stmt)

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_batch(cur, insert_stmt, data.values)
        conn.commit()
        log("Commited")
    except psycopg2.Error as e:
        diag = e.diag
        print("printing error diags (everything defined in psycopg package):")
        for attr in [
            'column_name', 'constraint_name', 'context', 'datatype_name',
            'internal_position', 'internal_query', 'message_detail',
            'message_hint', 'message_primary', 'schema_name', 'severity',
            'source_file', 'source_function', 'source_line', 'sqlstate',
            'statement_position', 'table_name', ]:
            v = getattr(diag, attr)
            print(attr, ":",  v)
        raise

    cur.close()
    conn.commit()

def write_csv(data, filename):
    log("appending data to " + filename)
    data.to_csv(args["csvfile"], mode='a', header=False)

if __name__ == '__main__':
    params = getconfig(CONFIG_PATH)

    parser = argparse.ArgumentParser(description='ElasticSearch extract utility to dump to CSV or Load to Postgres. '
                                                 '\nIf loading to Postgres, data is read from ES to local memory then inserted to remote database'
                                                  '\nConnect configuration for ES and Postgres is configured in esextract.conf'
                                                  '\nColumns (fields) of data are extracted from ElasticSearch based on spec. in cols.conf'
                                                  '\nIf writing to a database table, table-cols must match the structure of cols extracted from ElasticSearch '
                                                  '\nEXAMPLE - extract last days record (default range on @timestamp) and load to Postgres database table "my_schema.my_table":'
                                                  '\n    $> python esextract.py -n 1 -k jobStatus -f JOB_FINISH -d my_schema.my_table'
                                                  '\nEXAMPLE - extract a range of values based on an integer value for the endTime field and dump to CSV:'
                                                  '\n    $> python esextract.py -r 1541680814:1542967602 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv'
                                                  '\nEXAMPLE - extract a range of values from a start-point to the highest value (unbounded end of range):'
                                                  '\n    $> python esextract.py -r 1541680814 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv'
                                     , formatter_class=argparse.RawTextHelpFormatter)

    #parser.add_argument('-f', '--file', dest="filename", default=PWD_FILE, help='Specify encoded filename',
    #                    action='store')

    group_extract = parser.add_mutually_exclusive_group(required=True)
    group_extract.add_argument('-n', '--numdays', dest="numdays", help='Extract last n days')
    group_extract.add_argument('-r', '--range', dest="range", help='range extract <FROM>:<TO>')

    parser.add_argument('-s', '--searchkey', dest="searchkey", default="@timestamp",
                        help='key for range or numdays to search on - i.e. last-n based on @timestamp / range on epoc val', action='store')

    parser.add_argument('-k', '--key', dest="key", help='key for filtering on', action='store', required=True)
    parser.add_argument('-f', '--filter', dest="filter", help='value for filtering by', action='store',required=True)



    group_dest = parser.add_mutually_exclusive_group(required=True)
    group_dest.add_argument('-c', '--file', dest="csvfile", help='Save as CSV file', action='store')
    group_dest.add_argument('-d', '--datatable', dest="datatable",  help='Load to Database Table (DB config in esextract.conf)', action='store')
    # group.add_argument('-f', '--file', dest = "filename", default = PWD_FILE, help='Specify encoded filename', action='store')
    args = vars(parser.parse_args())

    # Archive the old logfile
    log_rotate()
    cols_file = params['colsfile']

    if args["numdays"]:
        print("Numdays set to",args["numdays"])
        extract = extract_data_days(filterkey=args["key"], filterval=args["filter"], rangefield=args["searchkey"], dayshist=args["numdays"])
        data = create_dataframe(extract, cols_file)
    elif args["range"]:
        print("Range set to", args["range"])
        startrange = args["range"].split(":")[0]
        if len(args["range"].split(":"))  == 2:
            endrange = args["range"].split(":")[1]
        else:
            endrange = None

        extract = extract_data_range(filterkey=args["key"], filterval=args["filter"], rangefield=args["searchkey"], startrange=startrange, endrange=endrange)
        data = create_dataframe(extract, cols_file)
    else:
        print("Unhandled mode - can only extract based on -n <numdays> or -r <startrange[:endrange]>")
        exit(1)

    if args["csvfile"]:
        log("Writing CSV data to " + args["csvfile"])
        #data.to_csv(args["csvfile"])
        write_csv(data, args["csvfile"])
    elif args["datatable"]:
        log("Inserting data to database table " + args["datatable"])
        dataframe_to_db(data, table_name=args["datatable"])
    else:
        print("Unhandled data target - specify either -c csvfile or -d database")
        exit(1)
