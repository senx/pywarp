# Introduction

The `PyWarp` module provides functions which ease the interaction with the [Warp 10](https://warp10.io/) Time Series Platform.

The functions it provides can be used to fetch data from a Warp 10 instance into a [Pandas](https://pandas.pydata.org) [dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) or a [Spark](https://spark.apache.org) [dataframe](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). A function is also provided for loading data from [HFiles](https://blog.senx.io/introducing-hfiles-cloud-native-infinite-storage-for-time-series-data/) into a Spark dataframe.

A function also allows the conversion of native Warp 10 *wrappers* into a Pandas dataframe.

An `exec` function allows the execution of WarpScript on a Warp 10 instance and the retrieval of the result.

# Installation

Simply run the following command:

```
python3 setup.py install
```

# Data Frame schema

The data frames returned by `pywarp.fetch` and `pywarp.spark.wrappers2df` have the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `class` | `STRING` | Class name of Geo Time Series (*GTS*) |
| `labels` | `MAP` | Map of labels of the GTS |
| `attribtues` | `MAP` | Map of attributes of the GTS |
| `ts` | `DOUBLE` | Timestamp of the data point, in time units since the Epoch |
| `lat` | `DOUBLE` | Latitude of the data point |
| `lon` | `DOUBLE` | Longitude of the data point |
| `elev` | `LONG` | Elevation of the data point, in mm |
| `l_value` | `LONG` | `LONG` value of the data point |
| `d_value` | `DOUBLE` | `DOUBLE` value of the data point |
| `b_value` | `BOOLEAN` | `BOOLEAN` value of the data point |
| `s_value` | `STRING` | `STRING` value of the data point |
| `bin_value` | `BINARY` | `BINARY` value of the data point |

# Examples

## Reading data from a Warp 10 instance

```
import pywarp

df = pywarp.fetch('https://HOST:PORT/api/v0/fetch', 'TOKEN', 'SELECTOR{}', 'now', -100)
print(df)
```

## Reading data from a Warp 10 instance via Spark

```
import pywarp.spark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("PyWarp Test").getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

df = pywarp.spark.fetch(sc, 'https://HOST:PORT/api/v0/fetch', 'TOKEN', 'SELECTOR{}', 'now', -1440)
df = pywarp.spark.wrapper2df(sc, df, 'wrapper')
df.show()
```

## Reading data from HFiles in Spark

```
import pywarp.spark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("PyWarp Test").getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

df = pywarp.spark.hfileread(sc, '/path/to/file.hfile', selector='SELECTOR{}', end=1081244481160.000, start=1081244472361.000)

df = pywarp.spark.wrapper2df(sc, df, 'wrapper')
df.show(n=1000,truncate=False)
```

## Executing WarpScript

```
import pywarp

x = pywarp.exec('https://sandbox.senx.io/api/v0/exec',
"""
REV REV REV "UTF-8" ->BYTES 42 42.0 F 6 ->LIST
#->PICKLE ->B64
""",
False # Set to true if your code returns base64 encoded pickled content (decomment the line that uses ->PICKLE above)
)

print(x)
```
