# Introduction

The `PyWarp` module provides functions which ease the interaction with the [Warp 10](https://warp10.io/) Time Series Platform.

The functions it provides can be used to fetch data from a Warp 10 instance into a [Pandas](https://pandas.pydata.org) [dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) or a [Spark](https://spark.apache.org) [dataframe](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). A function is also provided for loading data from [HFiles](https://blog.senx.io/introducing-hfiles-cloud-native-infinite-storage-for-time-series-data/) into a Spark dataframe.

A function also allows the conversion of native Warp 10 *wrappers* into a Pandas dataframe.

An `exec` function allows the execution of WarpScript on a Warp 10 instance and the retrieval of the result.

# Installation

In this folder run the command:
```
pip3 install -e .
```

# Fetching data options

Data points in the Warp 10 platform follow a Geo Time Series data model (geo location information is optional).
The PyWarp library provides various functions to fetch and represent these data points using dataframes:

- **`pywarp.fetch`**: returns a single dataframe where each row represents a single data point.
- **`pywarp.sfetch`**: returns a list of dataframes, with each dataframe representing a distinct (geo) time series.
- **`pywarp.ffetch`**: returns a single dataframe, resulting from the fusion of multiple (geo) time series dataframes.
- **`pywarp.exec`**: outputs the parsed JSON result of a WarpScript query.

A notebook example for each function is provided in `test/`.

# Data Frame Schemas

### 1. Data Point Stream Data Frame

Returned by `pywarp.fetch` and `pywarp.spark.wrappers2df`, this format streams data points within a single Pandas dataframe, where each row represents a distinct data point.

| Column Name | Data Type | Description | Optional |
|------------|-----------|-------------|----------|
| classname  | str       | Classname of the series the data point belongs to | No       |
| labels     | dict      | Labels of the series the data point belongs to | No       |
| attributes | dict      | Attributes of the series the data point belongs to | No       |
| ts         | int       | Timestamp of the data point in time units since Epoch | No       |
| lat        | float     | Latitude of the data point | No       |
| lon        | float     | Longitude of the data point | No       |
| elev       | int       | Elevation of the data point | No       |
| l_value    | int       | `LONG` value of the data point |No       |
| d_value    | float     | `DOUBLE` value of the data point | No       |
| b_value    | bool      | `BOOLEAN` value of data point | No       |
| s_value    | str       | `STRING` value of data point | No       |
| bin_value  | binary    | `BYTES` value of data point | No       |

### 2. GTS Data Frame List

Returned by `pywarp.sfetch`, this format gives a list of individual Pandas dataframes, each representing a unique Geo Time Series.

| Column Name | Data Type | Description | Optional |
|------------|-----------|-------------|----------|
| ts or *index* | int       | Timestamp in time units since Epoch | No       |
| lat        | float     | Latitude    | Yes      |
| lon        | float     | Longitude   | Yes      |
| elev       | int       | Elevation   | Yes      |
| `<classname>` | various   | Value       | No       |

Each DataFrame's `.attrs` dict contains:
  - **warp10classname**: Classname of the Geo Time Series (str).
  - **warp10labels**: Labels associated with the time series (dict).
  - **warp10attributes**: Attributes of the time series (dict).

### 3. Fused GTS Data Frames

Returned by `pywarp.ffetch`, this format amalgamates data from all fetched Geo Time Series into columns of a single Pandas dataframe.

| Column Name/Prefix      | Data Type | Description                             | Optional |
|-------------------------|-----------|-----------------------------------------|----------|
| *index*                 | int       | Timestamp in time units since Epoch      | No       |
| l:`<label key>`         | str       | One column for each unique label key     | Yes      |
| a:`<attribute key>`     | str       | One column for each unique attribute key | Yes      |
| lat:`<classname>`       | float     | Latitude, one column for each unique classname  | Yes      |
| lon:`<classname>`       | float     | Longitude, one column for each unique classname | Yes      |
| elev:`<classname>`      | int       | Elevation, one column for each unique classname | Yes      |
| val:`<classname>`       | various   | Value, one column for each unique classname     | No        |

### 4. WarpScript JSON Output

`pywarp.exec` returns the parsed JSON output of a WarpScript query obtained against the Warp 10 `/exec` endpoint.

This is the most flexible way to retrieve data in a customizable format.

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

builder = SparkSession.builder.appName("PyWarp Test")

spark = builder.getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

df = pywarp.spark.fetch(sc, 'https://HOST:PORT/api/v0/fetch', 'TOKEN', 'SELECTOR{}', 'now', -1440)
df = pywarp.spark.wrapper2df(sc, df, 'wrapper')
df.show()
```

Spark jobs making use of the HFStore extension must be launched using:

```
spark-submit --packages io.warp10:warp10-spark:3.0.2,io.senx:warp10-ext-hfstore:2.0.0 \
  --repositories https://maven.senx.io/repository/senx-public \
  --properties-file spark.conf \
  --files warp10.conf
```

where `spark.conf` contains the following definitions:

```
##
## Executor specific options
##

spark.executor.extraJavaOptions=-Dwarp10.config=warp10.conf -Ddisable.logging=true 

##
## Driver specific options
##

spark.driver.extraJavaOptions=-Dwarp10.config=warp10.conf -Ddisable.logging=true 
```

and the `warp10.conf` file contains *a minima*:

```
##
## Use microseconds as the time unit
##
warp.timeunits=us

##
## Load the Spark extension
##
warpscript.extensions=io.warp10.spark.SparkWarpScriptExtension

##
## Load the Debug extension so STDOUT is available
##
warpscript.extension.debug=io.warp10.script.ext.debug.DebugWarpScriptExtension
```

Alternatively if you do not want to use `spark-submit`, you can add the following in your script between the line `builder = ....` and `spark = builder.getOrCreate()`

```
conf = {}
conf['spark.master'] = 'local'
conf['spark.submit.deployMode'] = 'client'
conf['spark.executor.instances'] = '1'
conf['spark.executor.cores'] = '2'
conf['spark.driver.memory'] = '1g'
conf['spark.executor.memory'] = '1g'
conf['spark.executor.extraJavaOptions'] = '-Dwarp10.config=warp10.conf -Ddisable.logging=true'
conf['spark.driver.extraJavaOptions'] = '-Dwarp10.config=warp10.conf -Ddisable.logging=true'
conf['spark.driver.bindAddress'] = '0.0.0.0'
conf['spark.jars.packages'] = 'io.warp10:warp10-spark:3.0.2,io.senx:warp10-ext-hfstore:2.0.0'
conf['spark.jars.repositories'] = 'https://maven.senx.io/senx-public'
conf['spark.files'] = 'warp10.conf'

for (k,v) in conf.items():
  builder = builder.config(key=k,value=v)
```

and simply launch it using `python3`.

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

## Executing WarpScript on a Warp 10 instance

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
## Executing WarpScript in Spark

```
import pywarp

spark = SparkSession.builder.appName("PyWarp Test").getOrCreate()
sc = spark.sparkContext

df = ....

# Register a function 'foo' which returns a STRING and takes 2 parameters
pywarp.spark.register(df.sql_ctx, 'foo', 2, '')

# Create a temp view
df.createOrReplaceTempView('DF')
# Call WarpScript which converts column _1 to a STRING and returns it
df = df.sql_ctx.sql("SELECT foo(' TOSTRING', _1) AS str FROM DF");
df.show()
```
