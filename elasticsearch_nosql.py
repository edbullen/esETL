"""
Elasticsearch extract functions
"""

from elasticsearch import Elasticsearch
import esextract

SCROLL_SIZE = 10000

def extract_data_range(params, inputsource, filterkey, filterval, rangefield, startrange, endrange=None, cols_file=None,
                       csvfile=None, database_conf=None, equality=False):
    """
    Query ElasticSearch for a given filter and range-field with startrange and endrange vars
    Null endrange means scan to end.
    Either dump to CSV or write to db during loop through of batches of results from ES
    :param params - dictionary of params for this ES input source
    :param inputsource - this is a config ref to the ES Host to read data from
    :param filterkey:
    :param filterval:
    :param rangefield:
    :param startrange:
    :param endrange:
    :param cols_file: file that contains list of cols to extract and load
    :param csvfile:  path to file
    :param database_conf:  Config Identifier for Database
    :param equality: flag to switch on gte / lte equality range
    :return: number of records "n" processes
    """
    #sections = esextract.getconfig(CONFIG_PATH)
    #params = sections[inputsource]

    startrange = str(startrange)
    endrange = str(endrange)

    # Checks
    #if database_conf is None and csvfile is None:
    #    raise AttributeError('must specify csvfile or database')
    if database_conf and csvfile:
        raise AttributeError('cannot specify csvfile AND database')
    if cols_file is None:
        raise AttributeError('No Cols configuration specified')

    # Query Elastic Search
    esextract.log("Extract Data between range " + startrange + " and " + endrange + " for " + rangefield)
    if filterkey:
        esextract.log(" for filterkey:" + filterkey + " filterval:" + filterval)
    esextract.log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
    es = Elasticsearch([{u'host': params['elasticsearchhost'], u'port': params['elasticsearchport']}])

    extract = [] # extracted list of results
    n = 0  # number of records processed

    try:
        indexmask = params["indexmask"]
    except:
        indexmask ='*'

    #for index_name in es.indices.get('*'):
    for index_name in es.indices.get(indexmask):
        # scroll through results to handle > 10,000 records
        body_string = """{
                            "query": {
                      """
        if filterkey:
            body_string = body_string + """
                                "bool": {
                                    "must": [{
                                    "match": { "<filterkey>": "<filterval>"} } ,
                                    {
                                    """

        body_string = body_string + """
                                    "range": { "<rangefield>": {"gt": "<startrange>"
                                                           ,"lt": "<endrange>"
                                                          } 

                                             }
                                    """
        if filterkey:
            body_string = body_string + """
                                    }
                                    ]
                                    """
        body_string = body_string + """
                                }
                            }
                         }
                      """
        # Change to gte / lte equality range search
        if equality:
            body_string = body_string.replace("\"gt\"", "\"gte\"" )
            body_string = body_string.replace("\"lt\"", "\"lte\"")

        # replace the tags in the template ElasticSearch query with filter vals
        if filterkey:
            body_string = body_string.replace("<filterkey>", filterkey)
            body_string = body_string.replace("<filterval>", filterval)

        body_string = body_string.replace("<rangefield>", rangefield)
        body_string = body_string.replace("<startrange>", startrange)

        # if no end-range is specified, remove the "lte" part of range search - search to end
        if endrange == "None":
            esextract.log("No End-Range - scan to latest record")
            body_string = body_string.replace(",\"lte\": \"<endrange>\"", "")
            body_string = body_string.replace(",\"lt\": \"<endrange>\"", "")
        else:
            body_string = body_string.replace("<endrange>", endrange)

        ##esextract.log("DEBUG: dumping ES search-body string")
        ##esextract.log(body_string)

        page = es.search(index=index_name,
                         scroll='2m',
                         size=SCROLL_SIZE,
                         body=body_string)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        esextract.log("Index: " + index_name + " Total_Records:" + str(es.cat.indices(index_name).split()[6]) + " Hits:" + str(
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
            esextract.log("Extracted " + str(len(extract)) + " records")

            # write to database or CSV - create a Pandas Data frame and then write it out to DB/csv
            data = esextract.create_dataframe(extract, cols_file)
            if database_conf:
                #log("Inserting data to database table at " + database_conf)
                esextract.dataframe_to_db(data, database_conf)
            elif csvfile:
                # log("Writing CSV data to " + csvfile)
                esextract.write_csv(data, csvfile)
            else:
                esextract.write_stdout(data)

            # reset the extract list
            n = n + len(extract)
            extract = []
            esextract.log("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
    esextract.log("Total Data Extract and Load: " + str(n) + " records")

    return n

def extract_data_agg(params, filterkey='jobStatus', filterval='JOB_FINISH2', monthshist='now-12M', aggkey='cpuTime',
                     aggtype='sum'):
    """
    ** !! WIP - NEEDS a RE_WRITE **
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
    #params = esextract.getconfig(CONFIG_PATH)

    # Query Elastic Search
    esextract.log("Aggregate Data for last n months:" + monthshist + " filterkey:" + filterkey + " filterval:" + filterval)
    esextract.log("aggregating for " + aggkey)
    esextract.log("Query ElasticSearch at " + str(params['elasticsearchhost']) + " port " + str(params['elasticsearchport']))
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

    esextract.log("Returned " + str(len(results['aggregations']['group_by_month']['buckets'])) + " aggregations")

    for r in results['aggregations']['group_by_month']['buckets']:
        month = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['key'] / 1000))[:7]  # ES down-grades to EPOC
        val = round(r['aggValue']['value'], 2)
        row = [month, val]
        extract.append(row)

    return extract