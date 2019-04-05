"""

NoSQL (Elasticsearch "E" / Splunk "S" / JSON RestAPI data extract-and-load utility.

Query some cols (based on a config file) from noSQL datasource and create a pandas data-frame to either dump to CSV or load to database
Extract a range of data between two vals based on a range-key and also filter by (another) key - value pair

"""
# Python package imports
import pandas as pd
import io
import os
import sys
import math
import configparser
import time
import re
import datetime
import argparse
# Modules
import pwdutil  # utility for retreiving  password that is not stored in clear-text fmt.  Requires previous setup and .key file configuration
import elasticsearch_nosql # Elastics search data access functions
import postgres_db # Postgres DB functions

if os.environ.get('CONFIG_PATH'):
    CONFIG_PATH = os.environ.get('CONFIG_PATH')
else:
    CONFIG_PATH = os.getcwd() + "/conf/esextract.conf"
if os.environ.get('KEY_PATH'):
    KEY_PATH = os.environ.get('KEY_PATH')
else:
    KEY_PATH = os.getcwd() + "/conf" 

if os.environ.get('LOG_ROOT'):
    LOG_ROOT = os.environ.get('LOG_ROOT')
else:
    LOG_ROOT = os.getcwd() + "/log"

LOG_PATH = LOG_ROOT + "/esextract.log"
CSV_PATH = LOG_ROOT + "/esextract.csv"

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
        original = dataframe
        dataframe = dataframe.iloc[dataframe.astype(str).drop_duplicates().index]
        
        n_no_dup = len(dataframe)

    if n != n_no_dup:
        log("WARNING droppped duplicates: " + str(n - n_no_dup) )
        timestamp = gettimestamp(simple=True)
        log("Dumping original dataframe pre-dedupe")
        fname = LOG_ROOT + "/" + timestamp + "original_" + ".csv"
        write_csv(original, fname)
        log("Dumping dataframe de-duped")
        fname = LOG_ROOT + "/" + timestamp + "de_duped_" + ".csv"
        write_csv(dataframe, fname)

    log("   Dimensions: " + str(dataframe.shape))
    return dataframe

def get_database_conn(database_conf):
    conn = None
    try:
        sections = getconfig(CONFIG_PATH)
        params = sections[database_conf]
    except:
        raise ConfigNotFound(database_conf)
    username = params["dbusername"]
    host = params["dbhost"]
    port = params["dbport"]
    database = params["database"]
    type = params["type"]
    table_name = params["table"]

    # utility for retreiving obsfucated password from ./conf dir
    password = pwdutil.decode(pwdutil.get_key(KEY_PATH + "/.key_" + database_conf), pwdutil.get_pwd(pwdfile=KEY_PATH + "/.pwd_" + database_conf))

    # Database Connect
    if params["type"] == "postgres":
        conn = postgres_db.connection(username, password, host, port, database)
    else:
        log("Database Connect to " + params["type"] + " not supported")

    return conn, type, table_name


# def dataframe_to_db(data, table_name):
def dataframe_to_db(data, database_conf, batch_size=None):
    """
    pass a dataframe in for a bulk-insert to database
    :param data:  Pandas data-frame
    :param database_conf: Config Identifier that maps to database, host, port, username, tablename
    :return: (None)
    """

    # Get Database Connection, Database Type, Database Table Name
    conn, type, table_name = get_database_conn(database_conf)

    log("   Insert to database - table " + table_name)
    log("   Rows:" + str(len(data)))

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

    #Logic in loop to break up bulk of dataframe into sets of iterations
    n = len(data)
    if batch_size:
        iterations = math.ceil(len(data)/batch_size)
        iter_mod = n % batch_size  # modulus to factor in uneven tail of items at end of loop
    else:
        iterations = 1
        iter_mod = 0
        batch_size = n

    log("   Batch Size:       " + str(batch_size))
    log("   Batch Iterations: " + str(iterations))

    for i in range(0, iterations):

        if i == iterations - 1:
            #final pass, need to factor in the mod left-over for uneven final batch
            slice_start = (i * batch_size)
            slice_end = (i * batch_size + batch_size - iter_mod)
            data_slice = data[slice_start : slice_end]
        else:
            #print(i * batch_size, " ", i * batch_size + batch_size)
            slice_start = (i * batch_size)
            slice_end = (i * batch_size + batch_size)

            data_slice = data[slice_start : slice_end ]

        # Database Execute Insert an Numpy ndarray of Values = pandas df.values
        try:
            if type == "postgres":
                #postgres_db.insert_statement(conn, insert_stmt, data.values)
                postgres_db.insert_statement(conn, insert_stmt, data_slice.values, slice_start, slice_end)
            else:
                log("Database Connect to " + type + " not supported")
        except:
                timestamp = gettimestamp(simple=True)
                log("Failed Insert: " + insert_stmt)
                log("Dumping data-frame that failed to load to CSV file")
                fname = LOG_ROOT + "/" + "failed_" + timestamp + ".csv"
                write_csv(data_slice,fname)
                raise
    conn.close()

def merge_on_db(merge_target, database_conf, logPrintFlag=False):
    """
    Merge the data in "database_conf" INTO the merge_target using
       INSERT INTO merge_target SELECT * FROM database_conf EXCEPT SELECT * from merge_target
     - a different syntax is needed for Oracle (... MINUS SELECT * from merge_target)
    Very simple approach is used that is not necessarily the most efficient for large indexed tables.
    :param merge_target:
    :param database_conf:
    :return:
    """
    log("Merge " + database_conf + " into " + merge_target)
    # Get Database Connection, Database Type, Database Table Name
    conn, type, table_name = get_database_conn(database_conf)

    merge_stmt = "INSERT INTO {} SELECT * FROM {} EXCEPT SELECT * FROM {}".format(merge_target,table_name,merge_target)

    # Database Execute Query-Insert
    if type == "postgres":
        rowcount = postgres_db.insert_merge_statement(conn, merge_stmt, logPrintFlag)
        log("    " + str(rowcount) + " Rows")
    else:
        log("Database Connect to " + type + " not supported")

def delete_on_db(database_conf, logPrintFlag=False):
    """

    :param database_conf: delete data from the table associated with this configuration
    :param logPrintFlag:
    :return:
    """

    # Get Database Connection, Database Type, Database Table Name
    conn, type, table_name = get_database_conn(database_conf)
    log("Delete data from table at " + database_conf + " table name: " + table_name)

    delete_stmt = "DELETE FROM {}".format(table_name)
    # Database Execute Query-Insert
    if type == "postgres":
        rowcount = postgres_db.delete_statement(conn, delete_stmt, logPrintFlag)
        log("    " + str(rowcount) + " Rows")
    else:
        log("Database Connect to " + type + " not supported")


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

    # Get Database Connection, Database Type, Database Table Name
    conn, type, table_name = get_database_conn(database_conf)

    # Database Query
    if filterkey:
        select_stmt = "SELECT MAX( {} ) FROM {} WHERE {} = {}".format(search_key, table_name, filterkey, filterval)
    else:
        select_stmt = "SELECT MAX( {} ) FROM {} ".format(search_key, table_name)

    # Database Execute Query
    if type == "postgres":
        records = postgres_db.select_statement(conn, select_stmt, logPrintFlag)
    else:
        log("Database Connect to " + type + " not supported")

    if len(records) > 1:
        raise ValueError('Unexpected max-val, more than 1 record')
    maxval = records[0]
    return maxval


def write_csv(data, filename):
    """
    Write out a CSV file.  Don't include header as may be incrementally building up a CSV
    Write-Append to existing file
    """
    log("   Appending data to file " + filename)
    data.to_csv(filename, mode='a', header=False)

def read_csv(filename, drop_duplicates=True):
    """
    read in a CSV and output a Pandas data-frame.
    Warnings and Errors from Pandas passed back in messages_list
    Attempt to remove duplicates by default.
    """
    log("Loading data from file (first line is database cols spec) " + filename)
    #redirect STD-ERR and STD-OUT to catch warnings / errors from reading in CSV
    real_stdout = sys.stdout
    real_stderr = sys.stderr
    fake_stdout = io.StringIO()
    fake_stderr = io.StringIO()
    data = None
    try:
        sys.stdout = fake_stdout
        sys.stderr = fake_stderr
        data = pd.read_csv(filename, sep=',', error_bad_lines=False, warn_bad_lines=True, skipinitialspace=True)


    finally:
        sys.stdout = real_stdout
        sys.stderr = real_stderr
        message = fake_stdout.getvalue()
        message = message + fake_stderr.getvalue()

        fake_stdout.close()
        fake_stderr.close()

        if drop_duplicates:
            log("   Dropping duplicates")
            data = data.iloc[data.astype(str).drop_duplicates().index]

        message = message[2:] # remove b' prefix - seems to be left over from bytes convert to string
        message = message[:-2] # remove final '
        message_list = message.split('\\n')
        return data, message_list


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
    $> python esextract.py -i MyElasticSearch  -s endTime -r 1541680814#1542967602 -c ./range.csv
EXAMPLE - extract a range of values based for the endTime field and filter by jobStatus=JOB_FINISH and dump to CSV:
        $> python esextract.py -i MyElasticSearch -s endTime -r 1541680814#1542967602 -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of values from a start-point to the highest value (unbounded end of range):
    $> python esextract.py -i MyElasticSearch -s endTime -r 1541680814 -k jobStatus -f JOB_FINISH -c ./range.csv
EXAMPLE - extract a range of timestamp values
    $> python esextract.py -i AnOtherEsConfig -s @timestamp -r 2019-01-31T14:02:39.000Z#2019-02-01T14:02:39.000Z -k jobStatus -f JOB_FINISH2 -c ../test.csv
EXAMPLE - dump the config
    $> python esextract.py dumpparams

"""
    , formatter_class=argparse.RawTextHelpFormatter)

    #group_extract = parser.add_mutually_exclusive_group(required=True)
    #group_extract.add_argument('-n', '--numdays', dest="numdays", help='Extract last n days')
    group_input = parser.add_mutually_exclusive_group(required=False)
    group_input.add_argument('-i', '--input', dest="inputsource"
                        , help='Input Source for Data - as defined in config file.')
    group_input.add_argument('-cin', '--csvfile_in', dest="csvfile_in", action='store'
                        , help = 'Input data from CSV file - * first line of CSV specifies the column-names to load to database*')
    group_input.add_argument('-merge', '--merge', dest="merge_target", action='store'
                             , help='Merge data from -d "database_config_dest" to "merge_target" database table in same database ')
    group_input.add_argument('-delete', '--delete', dest="delete_target", action='store_true'
                             , help='Delete all data from -d "database_config_dest" ')

    parser.add_argument('-s', '--searchkey', dest="searchkey", default="@timestamp", action='store', required=False
                        , help='key for range to search on or find MAX of - defaults to @timestamp.')


    group_range = parser.add_mutually_exclusive_group(required=False)
    group_range.add_argument('-r', '--range', dest="range", help='range extract <FROM>#<TO> - use # as separator')
    group_range.add_argument('-m', '--max_val', dest="max_val", action='store_true'
                            , help='get maximum value for the [ -s ] searchkey in the DESTINATION data store')


    parser.add_argument('-k', '--key', dest="key", action='store', default=None
                        , help='optional key for filtering on')
    parser.add_argument('-f', '--filter', dest="filter", action='store', default=None
                        , help='value for filtering by')

    group_dest = parser.add_mutually_exclusive_group(required=True)
    group_dest.add_argument('-cout', '--file', dest="csvfile", action='store', default = None
                            , help='Save as CSV file')
    group_dest.add_argument('-d', '--database_config', dest="database_conf", action='store', default = None
                            , help='database destination as defined in config file')
    group_dest.add_argument('-p', '--print_output', dest="print", action='store_true', default=None
                            , help='Print output')

    parser.add_argument('-e', '--equality', dest="equality", action='store_true', default=False
                            , help='range equality (greater-than-Equal and less-then-Equal)')

    parser.add_argument('-b', '--batch_size', dest="batch_size", action='store', default=None
                        , help='specify an integer batch-size number - number of records to insert to database per batch iteration')

    args = vars(parser.parse_args())

    if not (args["inputsource"] or args["csvfile_in"] or args["merge_target"] or args["delete_target"]) and args["max_val"] is False:
        log("Input Source -i or -cin not specified.  Exiting")
        exit(1)
    elif args["inputsource"]:
        inputsource = args["inputsource"]
    elif args["csvfile_in"]:
        csvfile_in = args["csvfile_in"]

    log_rotate()  # Archive the old logfile

    if args["batch_size"]:
        batch_size = int(args["batch_size"])
    else:
        batch_size = None

    #need cols_file for data-load, for CSV import get them from the header
    if (args["max_val"] is False and args["csvfile_in"] is None and args["merge_target"] is None and args["delete_target"] is False):
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
    elif args["csvfile_in"]:
        data, message_list = read_csv(csvfile_in)
        for message in message_list:
            log("   " + message)
        dataframe_to_db(data, args["database_conf"], batch_size=batch_size)
    elif args["merge_target"]:
        #print("DEBUG Selected Merge Operation", args["merge_target"], " on ", args["database_conf"])
        merge_on_db(args["merge_target"], args["database_conf"], logPrintFlag=True)
    elif args["delete_target"]:
        #print("DEBUG Selected Delete Operation", args["database_conf"])
        delete_on_db(args["database_conf"], logPrintFlag=True)

    else:
        print("Unhandled mode selected")
        exit(1)
