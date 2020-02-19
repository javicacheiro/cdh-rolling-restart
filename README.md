# cdh-rolling-restart
Rolling restart script for CDH using Cloudera Manager's REST API

## Setup
Just edit the script and configure the following variables accordingly to your Cloudera Manager endpoint:
- API_URL
- USER
- PASSWD

The only module used outside the standard Python library is the requests module, if you use the Anaconda Python distribution it is already included. If not, you can install it using pip:

    pip install requests

## Usage
```
usage: rolling_restart.py [-h] [-d DELAY] [-s] [-t TYPE] [-l]
                          {zookeeper,oozie,hue,spark_on_yarn,hdfs,yarn,hive,hbase,kafka,sqoop_client,impala,flume}

positional arguments:
  {zookeeper,oozie,hue,spark_on_yarn,hdfs,yarn,hive,hbase,kafka,sqoop_client,impala,flume}
                        Service to restart

optional arguments:
  -h, --help            show this help message and exit
  -d DELAY, --delay DELAY
                        Delay between instance restarts
  -s, --staled          Restart only instances with staled configuration
  -t TYPE, --type TYPE  Instance type to restart for the given service
  -l, --list-types      List instace types for the given service
```

## Examples
Rolling restart of all YARN Node Managers:

    python rolling_restart.py -t NODEMANAGER yarn

Rolling restart of all HBase Region Servers with a delay of 30 seconds:

    python rolling_restart.py -d 30 -t REGIONSERVER hbase

Rolling restart only of instances with staled configuration of Impala:

    python rolling_restart.py -s impala

List all instance types available for a given service:

    python rolling_restart.py -l yarn
