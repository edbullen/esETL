"""

NoSQL (Elasticsearch "E" / Splunk "S" / JSON RestAPI data extract-and-load utility.

Query some cols (based on a config file) from noSQL datasource and create a pandas data-frame to either dump to CSV or load to database
Extract a range of data between two vals based on a range-key and also filter by (another) key - value pair

"""

import pandas as pd
import os
import sys
import configparser
import time
import re
import datetime
import argparse

import pwdutil  # utility for retreiving  password that is not stored in clear-text fmt.  Requires previous setup and .key file configuration
import elasticsearch_nosql # Elastics search data access functions
import postgres_db # Postgres DB functions

CONFIG_PATH = os.getcwd() + "/conf/esextract.conf"
KEY_PATH = os.getcwd() + "/conf" # just specify the directory, not filename
LOG_ROOT = os.getcwd() + "/log/"
LOG_PATH = LOG_ROOT + "esextract.log"
CSV_PATH = os.getcwd() + "/log/esextract.csv"

class ConfigFileAccessError(Exception):
    pass
class ConfigFileParseError(Exception):
    pass
class DataFrameColsSpecification(Exception):
    pass
class DataExtractSourceClass(Exception):
    pass
class ConfigNotFound(Exception):
    pass

def fileexists(fname):
    return (os.path.isfile(fname))

def gettimestamp(simple=False):
    if simple:
        return str(datetime.datetime.now().strftime("%Y%m%d_%H%M"))
    else:
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


def log(text, printFlag=True):
    if printFlag:
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

def epoc_to_date(epoc_timestamp):
    pass


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
                       csvfile=None, database_conf=None, equality = False):
    """
    function to call the correct NoSQL data-store (ES / Splunk etc)
    :param inputsource:
    :param filterkey:
    :param filterval:
    :param rangefield:
    :param startrange:
    :param endrange:
    :param cols_file: Specify the location of a file containing list of cols to extract
    :param csvfile:
    :param database_conf:
    :return:
    """

    sections = getconfig(CONFIG_PATH)
    params = sections[inputsource]

    if params["class"] == "elasticsearch" :
        n = elasticsearch_nosql.extract_data_range(params, inputsource, filterkey, filterval, rangefield, startrange, endrange, cols_file, csvfile, database_conf, equality)
    else:
        raise DataExtractSourceClass("unhandled class of extract type")

    return n


def create_dataframe(extract, cols_file=None, cols=None, drop_duplicates=True):
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
    n = len(dataframe)

    cols_list = re.sub(r' +', '', (str(dataframe.columns).replace(',', '\n').replace('\n\n', '\n')))
    cols_list = re.sub(r'\(\[', '\n', cols_list)
    cols_list = re.sub(r']\n', '', cols_list).replace("dtype='object')", "").replace("Index", "Columns:")

    if drop_duplicates:
        dataframe = dataframe.iloc[dataframe.astype(str).drop_duplicates().index]
        #duplicates = dataframe.duplicated()
        duplicates = dataframe.astype(str).duplicated()
        duplicate_count = len(duplicates[duplicates].index)
        if duplicate_count > 0:
            log("   " + str(duplicate_count) + " duplicates found in dataframe")
            log(str(duplicates[duplicates].index))
        n_no_dup = len(dataframe)

    if n != n_no_dup:
        log("WARNING droppped duplicates: " + str(n - n_no_dup) )
        log("    Dataframe Head: " + str(dataframe.head(1)))
        log("    Dataframe End : " + str(dataframe.tail(1)))
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
    try:
        sections = getconfig(CONFIG_PATH)
        params = sections[database_conf]
    except:
        raise ConfigNotFound(database_conf)
    username = params["dbusername"]
    host = params["dbhost"]
    port = params["dbport"]
    database = params["database"]
    table_name = params["table"]

    # utility for retreiving obsfucated password from ./conf dir
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
    try:
        if params["type"] == "postgres":
            postgres_db.insert_statement(conn, insert_stmt, data.values)
        else:
            log("Database Connect to " + params["type"] + " not supported")
    except:
            timestamp = gettimestamp(simple=True)
            log("Dumping data-frame that failed to load to CSV file")
            fname = LOG_ROOT + "failed_" + timestamp + ".csv"
            write_csv(data,fname)
            raise


def maxval_from_db(search_key, database_conf, filterkey, filterval, logPrintFlag=False ):
    """
    :param search_key: the field to select range on - MAX
    :param filterkey: the key to filter results on
    :param filterval: the value to filter by
    :param database_conf: the database spec to connect to
    :return: MAX val for search_key - STRING value (we don't know what type this is)
    """
    maxval = ""

    log("Get max-val for " + search_key, logPrintFlag)

    try:
        sections = getconfig(CONFIG_PATH)
        params = sections[database_conf]
    except:
        raise ConfigNotFound(database_conf)

    username = params["dbusername"]
    host = params["dbhost"]
    port = params["dbport"]
    database = params["database"]
    table_name = params["table"]

    # utility for retreiving obsfucated password from ./conf dir
    password = pwdutil.decode(pwdutil.get_key(KEY_PATH + "/.key_" + database_conf),
                              pwdutil.get_pwd(pwdfile=KEY_PATH + "/.pwd_" + database_conf))

    # Database Connect
    if params["type"] == "postgres":
        conn = postgres_db.connection(username, password, host, port, database)
    else:
        log("Database Connect to " + params["type"] + " not supported")

    # Database Query
    if filterkey:
        select_stmt = "SELECT MAX( {} ) FROM {} WHERE {} = {}".format(search_key, table_name, filterkey, filterval)
    else:
        select_stmt = "SELECT MAX( {} ) FROM {} ".format(search_key, table_name)

    # Database Execute Query
    if params["type"] == "postgres":
        records = postgres_db.select_statement(conn, select_stmt, logPrintFlag)
    else:
        log("Database Connect to " + params["type"] + " not supported")

    if len(records) > 1:
        raise ValueError('Unexpected max-val, more than 1 record')
    maxval = records[0]
    return maxval


def write_csv(data, filename):
    log("   Appending data to " + filename)
    data.to_csv(filename, mode='a', header=False)

def write_stdout(data):
    cols = list(data)
    n = len(cols)
    log("   Printing to terminal: ")
    for idx,row in data.iterrows():
        str_out = ""
        for i in range(0, n):
            #print(row[i],", ", end="")
            str_out = str_out + str(row[i]) + ","
        str_out = str_out[:-1] #remove last comma
        print(str_out)


if __name__ == '__main__':
    params = getconfig(CONFIG_PATH)

    if len(sys.argv) > 1:
        if sys.argv[1] == "dumpparams":
            for key, value in params.items():
                print(key)
                for sectionkey, sectionval in params[key].items():
                    print("\t", sectionkey, ":", sectionval)

            exit(0)

    parser = argparse.ArgumentParser(description="""** ElasticSearch extract utility to dump to CSV or Load to Postgres Database. ** 

If loading to Postgres, data is read from ES to local memory then inserted to remote database
Connect configuration for ES and Postgres is configured in esextract.conf

Read in from an input "-i" and write out to a datastore "-d" or "-c" for csv local file.

Columns (fields) of data are extracted from ElasticSearch based on spec. in cols.conf
If writing to a database table, table-cols must match the structure of cols extracted from ElasticSearch 

EXAMPLE - extract a range of values based on an integer value for the endTime field and dump to CSV:
    $> python esextract.py -i MyElasticSearch -r 1541680814#1542967602 -s endTime  -c ./range.csv
EXAMPLE - extract a range of values based for the endTime field and filter by jobStatus=JOB_FINISH and dump to CSV:
        $> python esextract.py -i MyElasticSearch -r 1541680814#1542967602 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of values from a start-point to the highest value (unbounded end of range):
    $> python esextract.py -i MyElasticSearch -r 1541680814 -s endTime -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of timestamp values
    $> python esextract.py -i AnOtherEsConfig -r 2019-01-31T14:02:39.000Z#2019-02-01T14:02:39.000Z -k jobStatus -f JOB_FINISH2 -c ../test.csv
EXAMPLE - dump the config
    $> python esextract.py dumpparams

"""
    , formatter_class=argparse.RawTextHelpFormatter)

    #group_extract = parser.add_mutually_exclusive_group(required=True)
    #group_extract.add_argument('-n', '--numdays', dest="numdays", help='Extract last n days')
    parser.add_argument('-i', '--input', dest="inputsource"
                        , help='Input Source for Data - as defined in config file.')

    group_range = parser.add_mutually_exclusive_group(required=True)
    group_range.add_argument('-r', '--range', dest="range", help='range extract <FROM>#<TO> - use # as separator')
    group_range.add_argument('-m', '--max_val', dest="max_val", action='store_true'
                            , help='get maximum value for the [ -s ] searchkey in the DESTINATION data store')

    parser.add_argument('-s', '--searchkey', dest="searchkey", default="@timestamp", action='store', required=True
                        , help='key for range to search on or find MAX of - defaults to @timestamp.  This is different to optional filterclause.')

    parser.add_argument('-k', '--key', dest="key", action='store', default=None
                        , help='optional key for filtering on')
    parser.add_argument('-f', '--filter', dest="filter", action='store', default=None
                        , help='value for filtering by')

    group_dest = parser.add_mutually_exclusive_group(required=True)
    group_dest.add_argument('-c', '--file', dest="csvfile", action='store', default = None
                            , help='Save as CSV file')
    group_dest.add_argument('-d', '--database_config', dest="database_conf", action='store', default = None
                            , help='database destination as defined in config file')
    group_dest.add_argument('-p', '--print_output', dest="print", action='store_true', default=None
                            , help='Print output')

    parser.add_argument('-e', '--equality', dest="equality", action='store_true', default=False
                            , help='range equality (greater-than-Equal and less-then-Equal')

    args = vars(parser.parse_args())

    if not args["inputsource"] and args["max_val"] is False:
        log("Input Source (-i) not specified - this must map to a configuration in the .conf file.  Exiting")
        exit(1)
    else:
        inputsource = args["inputsource"]

    log_rotate()  # Archive the old logfile
    if args["max_val"] is False:
        cols_file = params[inputsource]["colsfile"]  # Cols spec to extract data for (and load cols spec for DB / csv)

    if args["csvfile"]:
        if fileexists(args["csvfile"]):
            log("CSV file already exists - exiting")
            exit(1)

    if args["range"]:
        print("Range set to", args["range"])

        #split range on "#" to hopefully avoid chars that appear in the key - else need a more sophisticated regex
        startrange = args["range"].split("#")[0]
        if len(args["range"].split("#")) == 2:
            endrange = args["range"].split("#")[1]
        else:
            endrange = None

        n = extract_data_range(  inputsource=inputsource
                               , filterkey=args["key"]
                               , filterval=args["filter"]
                               , rangefield=args["searchkey"]
                               , startrange=startrange
                               , endrange=endrange
                               , cols_file=cols_file
                               , csvfile=args["csvfile"]
                               , database_conf=args["database_conf"]
                               , equality=args["equality"]
                               )

    elif args["max_val"]:
        #print("running a select statement to select max val for" + args["searchkey"])
        maxval = maxval_from_db(args["searchkey"], args["database_conf"], args["key"], args["filter"], logPrintFlag=False )
        log(maxval, False)
        print(maxval)
    else:
        print("Unhandled mode selected")
        exit(1)
