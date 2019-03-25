## ElasticSearch ETL Tool ##

Utility to extract from NoSQL and REST-endpoint datastores and load to database or extract to CSV.

#### Data Sources and Destinations ####

These are configured in the 
```./conf/esextract.conf```
file.


**Examples:**

1. Extract a range of values based on an integer value for the endTime field and dump to CSV:
   ```python esextract.py -i MyElasticSearch -r 1541680814#1542967602 -s endTime  -c ./range.csv```
2. Extract a range of values based for the endTime field and filter by jobStatus=COMPLETE and dump to CSV:
   ```python esextract.py -i MyElasticSearch -r 1541680814#1542967602 -s endTime -k jobStatus -f COMPLETE -c ./range.csv```
3. Extract a range of values from a start-point to the highest value (unbounded end of range):
    ```python esextract.py -i MyElasticSearch -r 1541680814 -s endTime -k jobStatus -f COMPLETE -c ./range.csv```
4. Extract a range of timestamp values
    ```python esextract.py -i AnOtherEsConfig -r 2019-01-31T14:02:39.000Z#2019-02-01T14:02:39.000Z -k jobStatus -f COMPLETE -c ../test.csv```
5. dump the config
    ```python esextract.py dumpparams```
6. Get the max value for a given key in a target database:
    ```python esextract.py -m -s myKeyField -d DatabaseTargetConfig```
    
    
#### Password Config ####
Password details for database servers / REST API are stored in a file
.key_<DataSourceName>


