#!/usr/bin/env python3
#
#   Copyright 2022-2023  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import requests
import base64
import math

import pandas
import pyspark

from collections import OrderedDict
from urllib.parse import unquote
from urllib.parse import urlparse
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

import time

def fetch(sc, endpoint, token, selector, end, timespan, conf = None):
  """
Read wrappers from a Warp 10 instance.

The time units used in end and timespan are those of the accessed platform.
  """

  ##
  ## Configuration used to fetch data from a Warp 10 instance
  ##

  if endpoint.endswith('/api/v0/fetch'):
    endpoint = endpoint[:-13]

  url = urlparse(endpoint)

  if not url.port:
    print('Endpoint MUST contain a port')
    exit(1)

  if not conf:
    conf = {}

    ##
    ## This defines your Warp 10 instance
    ##

    # This is the host and port of your Warp 10 instance
    conf['warp10.fetcher.fallbacks'] = url.hostname
    conf['warp10.fetcher.port'] = str(url.port)

    conf['warp10.fetcher.path'] = url.path + '/api/v0/sfetch'
    conf['warp10.fetcher.fallbacksonly'] = 'true'
    conf['warp10.fetcher.protocol'] = url.scheme
    conf['http.header.now'] = 'X-Warp10-Now'
    conf['http.header.timespan'] = 'X-Warp10-Timespan'

    ##
    ## Ensure your Warp 10 instance is configured with
    ## standalone.splits.enable = true
    ##
    conf['warp10.splits.endpoint'] = url.scheme + '://' + url.hostname + ':' + str(url.port) + url.path + '/api/v0/splits'

    # Maximum number of splits to generate
    conf['warp10.max.splits'] = '128'

    conf['warp10.http.connect.timeout'] = '60000'
    conf['warp10.http.read.timeout'] = '60000'

  # Token to use for fetching data
  conf['warp10.splits.token'] = token
  conf['warp10.splits.selector'] = selector

  # We fetch a single data point from the GTS, this could be an actual timespan if it were a positive value
  conf['warp10.fetch.now'] = str(end)
  conf['warp10.fetch.timespan'] = str(timespan)

  ##
  ## The Warp10InputFormat will return tuples (pairs) with an id (16 bytes as an hexadecimal STRING) of GTS and a wrapper containing a chunk of the said GTS.
  ##

  rdd = sc.newAPIHadoopRDD('io.warp10.hadoop.Warp10InputFormat', 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.BytesWritable', conf=conf)
  df = rdd.toDF()
  df = df.select(col('_2').alias('wrapper'))

  return df

def hfileread(sc, files, conf=None, selector=None, start=None, end=None, skip=None, count=None, keepEmpty=False, keys=None):
  """
Reads data from HFiles and put the read wrappers inside of a data frame.

The end and start parameters are specified in the configured time unit.
  """

  if not conf:
    conf = {}

  if isinstance(files, list):
    conf['mapreduce.input.fileinputformat.inputdir'] = ','.join(files)
  else:
    conf['mapreduce.input.fileinputformat.inputdir'] = str(files)

  conf['hfileinputformat.cells'] = 'false'
  conf['hfileinputformat.infos'] = 'false'

  if str == type(keys):
    conf['hfileinputformat.keys'] = keys

  if SQLContext == type(sc):
    # Register a dummy function so Warp 10 config gets initialized
    sc.registerJavaFunction(' ', 'io.warp10.spark.WarpScriptUDF1', NullType())
    sc = sc._sc
  else:
    sqlc = SQLContext(sc)
    sqlc.registerJavaFunction(' ', 'io.warp10.spark.WarpScriptUDF1', NullType())

  mc2 = """
  // Variable to keep track of the first record so we can emit a dummy record
  // to speed up schema inference, Spark issueing a take(1) on the RDD
  T 'first' CSTORE
  '@@SELECTOR@@' 'selector' STORE
  @@TIMECLIP@@ 'timeclip' STORE
  NOT
  <%  
    // Discard the key
    SWAP DROP


    // Extract the metadata, creating an empty encoder if extraction failed
    'raw' STORE $raw <% UNWRAPEMPTY WRAPFAST UNWRAPENCODER %> <% ERROR 1 SNAPSHOTN STDOUT NEWENCODER %> <% %> TRY

    // Apply selector if specified
    $selector '' !=
    <%
      $selector 
      <%
        SWAP CLONEEMPTY 0 NaN NaN NaN T ADDVALUE ->GTS
        'BOOLEAN' GET 1 ->LIST SWAP filter.byselector [] SWAP 3 ->LIST FILTER
        SIZE 0 !=
      %>
      EVAL NOT <% NULL %> <% $raw %> IFTE
    %> 
    <%
      DROP $raw
    %>
    IFTE
    // We can discard raw now
    'raw' FORGET
    DUP ISNULL NOT $timeclip AND <%
      UNWRAPENCODER
      @@END@@ @@START@@ TIMECLIP
    %> IFT

    // Continue processing only if METAMATCH matched (i.e. top of the stack is not NULL)
    DUP ISNULL NOT
    <%
      DUP TYPEOF 'ENCODER' != <% UNWRAPENCODER %> IFT
      @@SKIP@@ 'skip' STORE
      @@COUNT@@ 'count' STORE
      $skip 0 != $count MAXLONG != OR
      <%
        DUP CLONEEMPTY SWAP
        <%
          $skip 0 ==
          <%
            $count 0 > <% LIST-> DROP ADDVALUE $count 1 - 'count' STORE %> <% DROP BREAK %> IFTE
          %>
          <% DROP $skip 1 - 'skip' STORE %>
          IFTE
        %> FOREACH
      %> IFT
    %> IFT
    DUP ISNULL NOT
    <%
      DUP SIZE 0 != @@KEEPEMPTY@@ OR
      <%
        WRAPRAW
      %>
      <%
        DROP
      %> IFTE
    %>
    <%
      DROP
    %> IFTE
    'raw' FORGET
    // Emit a dummy record fast so Spark can determine the schema fast
    DEPTH 0 != <% '' SWAP 2 ->LIST %> <% $first <% [ 'x' NEWENCODER WRAPRAW ] %> IFT %> IFTE
    F 'first' STORE
  %>
  <%
    // Reset 'first'
    T 'first' STORE
  %>  IFTE
  """

  if selector:
    mc2 = mc2.replace('@@SELECTOR@@', selector)
  else:
    mc2 = mc2.replace('@@SELECTOR@@', '')
    
  if start:
    mc2 = mc2.replace("@@START@@", str(start) + " TOLONG ISO8601")
  else:
    mc2 = mc2.replace("@@START@@", "MINLONG ISO8601")

  if end:
    mc2 = mc2.replace("@@END@@", str(end) + " TOLONG ISO8601")
  else:
    mc2 = mc2.replace("@@END@@", "MAXLONG ISO8601")

  if end or start:
    mc2 = mc2.replace('@@TIMECLIP@@', 'T')
  else:
    mc2 = mc2.replace('@@TIMECLIP@@', 'F')

  if keepEmpty:
    mc2 = mc2.replace('@@KEEPEMPTY@@', 'T')
  else:
    mc2 = mc2.replace('@@KEEPEMPTY@@', 'F')

  if skip and skip < 0:
    print('skip cannot be negative')
    exit(1)
  elif not skip:
    skip = 0

  if None != count or None != skip:
    if None == count:
      count = 'MAXLONG'

  if type(count) == 'int' and count < 0:
    mc2 = mc2.replace("@@SKIP@@", "DUP SIZE " + str(skip - count) + " -")
    mc2 = mc2.replace("@@COUNT@@", str(-count))
  else:
    mc2 = mc2.replace("@@SKIP@@", str(skip))
    mc2 = mc2.replace("@@COUNT@@", str(count))


  if selector or start or end or skip or count:
    conf['warpscript.inputformat.class'] = 'io.senx.hadoop.HFileInputFormat'
    conf['warpscript.inputformat.script'] = mc2

    rdd = sc.newAPIHadoopRDD(inputFormatClass='io.warp10.spark.SparkWarpScriptInputFormat',
            keyClass='org.apache.hadoop.io.Text',
            valueClass='org.apache.hadoop.io.BytesWritable',
            conf=conf)

    df = rdd.toDF()
  else:
    rdd = sc.newAPIHadoopRDD('io.senx.hadoop.HFileInputFormat', 'org.apache.hadoop.io.BytesWritable', 'org.apache.hadoop.io.BytesWritable', conf=conf)
    df = rdd.toDF()

  # Remove dummy records
  df = df.filter("_1 != 'x'").select(col('_2').alias('wrapper'))

  return df

def wrapper2df(sc, df, col):
  """
Convert a data frame with wrappers into a data frame of observations.
  """

  mc2 = """
  /*
  ** Unwrap wrapper into an encoder
  */

  UNWRAPENCODER
  'encoder' STORE
  $encoder NAME 'class' STORE
  $encoder LABELS 'labels' STORE
  $encoder ATTRIBUTES 'attributes' STORE

  /* 
   * Build the list of data points
   */

  []
  $encoder
  <%
    LIST-> DROP
    [ 'ts' 'lat' 'lon' 'elev' 'val' ] STORE
    $lat ISNaN <% NULL 'lat' STORE NULL 'lon' STORE %> IFT
    $elev ISNaN <% NULL 'elev' STORE %> IFT
    [
      $ts $lat $lon $elev
      $val TYPEOF 'type' STORE
      <% $type 'LONG' == %> <% $val NULL NULL NULL NULL %>
      <% $type 'DOUBLE' == %> <% NULL $val NULL NULL NULL %>
      <% $type 'BOOLEAN' == %> <% NULL NULL $val NULL NULL %>
      <% $type 'STRING' == %> <% NULL NULL NULL $val NULL %>
      <% $type 'BINARY' == %> <% NULL NULL NULL NULL $val %>
      <% 'INVALID TYPE' MSGFAIL %>
      5 SWITCH
    ] 
    ->SPARKROW
    +!
  %>
  FOREACH
  [ $class $labels $attributes 5 ROLL ] ->SPARKROW
"""

  mc2 = mc2.replace('\n',' ').replace('\'','\\\'').replace('\"','\\\"')

  ##
  ## Schema resulting from the wrapper processing
  ##

  schema = StructType([
    StructField('class', StringType(), False),
    StructField('labels', MapType(StringType(), StringType()), False),
    StructField('attributes', MapType(StringType(), StringType()), False),
    StructField('datapoints', ArrayType(StructType([
      StructField('ts', LongType(), False),
      StructField('lat', DoubleType(), True),
      StructField('lon', DoubleType(), True),
      StructField('elev', LongType(), True),
      StructField('l_value', LongType(), True),
      StructField('d_value', DoubleType(), True),
      StructField('b_value', BooleanType(), True),
      StructField('s_value', StringType(), True),
      StructField('bin_value', BinaryType(), True),
    ])), False)
  ])

  ##
  ## Register a function to process the wrappers
  ##

  df.sql_ctx.registerJavaFunction("pywarp_wrapper2df", "io.warp10.spark.WarpScriptUDF2", schema)

  ##
  ## Extract wrapper content
  ##

  DF = 'DF' + str(int(time.time() * 1000000.0))
  df.createOrReplaceTempView(DF)
  df = df.sql_ctx.sql("SELECT pywarp_wrapper2df('" + mc2 + "', " + col + ") AS gts FROM " + DF)

  ##
  ## Explode the data points
  ##

  df = df.select('gts.class','gts.labels','gts.attributes', explode(df.gts.datapoints).alias('datapoint'))

  ##
  ## Flatten the dataframe
  ##

  (major,minor,patch) = sc.version.split('.')

  if int(major) <= 3 and int(minor) < 2:
    df = df.selectExpr('`gts.class` as class', '`gts.labels` as labels', '`gts.attributes` as attributes', 'datapoint.ts as ts', 'datapoint.lat as lat', 'datapoint.lon as lon', 'datapoint.elev as elev', 'datapoint.l_value as l_value', 'datapoint.d_value as d_value', 'datapoint.b_value as b_value', 'datapoint.s_value as s_value','datapoint.bin_value as bin_value')
  else:
    df = df.selectExpr('class', 'labels', 'attributes', 'datapoint.ts as ts', 'datapoint.lat as lat', 'datapoint.lon as lon', 'datapoint.elev as elev', 'datapoint.l_value as l_value', 'datapoint.d_value as d_value', 'datapoint.b_value as b_value', 'datapoint.s_value as s_value','datapoint.bin_value as bin_value')

  return df


def schema(obj):
  """
Returns a schema to which obj will conform, using primitive types supported by WarpScript (LONG, DOUBLE, STRING, BINARY, BOOLEAN)
  """

  if StructType == type(obj) or ArrayType == type(obj) or MapType == type(obj) or LongType == type(obj) or DoubleType == type(obj) or StringType == type(obj) or BinaryType == type(obj) or BooleanType == type(obj):
    return obj
  elif None == obj:
    return NullType()
  elif OrderedDict == type(obj): # MUST appear before dict
    fields = []
    for key, value in obj.items():
      fields.append(StructField(key, schema(value), True))
    objschema = StructType(fields, True)
  elif list == type(obj):
    objschema = ArrayType(schema(obj[0]))
  elif dict == type(obj):
    (k,v) = obj.popitem()
    objschema = MapType(schema(k), schema(v))
  elif int == type(obj):
    objschema = LongType()
  elif str == type(obj):
    if '' == str:
      objschema = StringType()
    else:
      objschema = pyspark.sql.types._parse_datatype_string(obj)
  elif bytes == type(obj):
    objschema = BinaryType()
  elif float == type(obj):
    objschema = DoubleType()
  elif bool == type(obj):
    objschema = BooleanType()
  else:
    raise RuntimeError('Unsupported type ' + repr(type(obj)))

  return objschema

def register(sqlc, name, argc, output_schema):
  """
Registers a function with the Spark SQL context so it can be used in a SELECT statement.

name is the name of the function as it will be used in the SELECT statement.
args is the number of parameters to supply the function in the SELECT statement, the first one being the WarpScript code to execute. The number of parameters must be between 1 and 22.
output_schema is either a Spark schema or an example object returned by the function call. If output_schema is a non empty string, it is assumed to be a Spark schema definition that will be parsed.
  """
  if argc < 1 or argc > 22:
    raise RuntimeError('Functions can only have from 1 to 22 arguments.')

  sqlc.registerJavaFunction(name, 'io.warp10.spark.WarpScriptUDF' + str(argc), schema(output_schema))
