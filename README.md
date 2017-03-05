# accumulo-delimited-ingest
An ingest client for reading delimited data and ingesting it into Apache Accumulo


# Usage

```
Usage: <main class> [options]
  Options:
  * -c, --columnMapping
      Comma-separated value of the mapping to the Accumulo schema
    --config
      Path to the Accumulo client configuration file
    -h, --help, -help

      Default: false
  * -i, --input
      The input data (files, directories, URIs)
    --instance
      The Accumulo instance name
    -p, --password
      The password for the Accumulo user
    -q, --quotedValues
      Are the values on each line quoted (default=false)
      Default: false
  * -t, --table
      The Accumulo table to ingest data into
  * -u, --username
      The user to connect to Accumulo as
    -zk, --zookeepers
      ZooKeeper servers used by Accumulo
```

## Column Mapping specification:

The column mapping defines how the columns in the CSV file are mapped to the Accumulo data. The mapping
is a comma-separated list of elements of the form `family[:qualifier]`. One of these elements in this
comma-separated list *must* be the "meta-element" `!rowId`. The rowId element defines the value in that
position in the CSV file to be the Accumulo RowID for all the Key-Value pairs generated from that
CSV row.

For example, consider the following CSV record:

```
12345,Josh,Elser,28,75,Male
```

Given a column mapping of `!rowId,person:first_name,person:last_name,person:age,person:height,person:sex`,
the following data in Accumulo would be generated:

```
12345 person:age []    28
12345 person:first_name []    Josh
12345 person:height []    75
12345 person:last_name []    Elser
12345 person:sex []    Male
```
