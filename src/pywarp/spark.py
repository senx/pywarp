#!/usr/bin/env python3
#
#   Copyright 2022  SenX S.A.S.
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

from urllib.parse import unquote
from urllib.parse import urlparse
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

import time

def fetch(sc, endpoint, token, selector, end, timespan, conf = None):
  """
Read wrappers from a Warp 10 instance.
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

def hfileread(sc, files, conf=None, selector=None, start=None, end=None):
  """
Reads data from HFiles and put the read wrappers inside of a data frame.
  """

  if not conf:
    conf = {}
    if isinstance(files, list):
      conf['mapreduce.input.fileinputformat.inputdir'] = ','.join(files)
    else:
      conf['mapreduce.input.fileinputformat.inputdir'] = str(files)
    conf['hfileinputformat.cells'] = 'false'

  rdd = sc.newAPIHadoopRDD('io.senx.hadoop.HFileInputFormat', 'org.apache.hadoop.io.BytesWritable', 'org.apache.hadoop.io.BytesWritable', conf=conf)
  df = rdd.toDF()

  if selector or start or end:
    df.sql_ctx.registerJavaFunction("pywarp_hfileread", "io.warp10.spark.WarpScriptUDF2", BinaryType())
    mc2 = "'raw' STORE $raw <% UNWRAPEMPTY WRAPFAST UNWRAPENCODER %> <% NEWENCODER %> <% %> TRY"
    macro = """
<%
  'METAMATCH' SECTION
  SWAP CLONEEMPTY 0 NaN NaN NaN T ADDVALUE ->GTS
  'BOOLEAN' GET 1 ->LIST SWAP filter.byselector [] SWAP 3 ->LIST FILTER
  SIZE 0 !=
  'TOP' SECTION
%> 'METAMATCH' STORE
    """
    mc2 = mc2 + macro

    if selector:
      mc2 = mc2 + " '" + selector + "' @METAMATCH NOT <% NULL %> <% $raw %> IFTE "
      mc2 = mc2.replace('\n',' ').replace('\'','\\\'').replace('\"','\\\"')

      DF = 'DF' + str(int(time.time() * 1000000.0))
      df.createOrReplaceTempView(DF)
      df = df.sql_ctx.sql("SELECT pywarp_hfileread('" + mc2 + "', _2) AS _2 FROM " + DF)
      df = df.na.drop()
      mc2 = 'UNWRAPENCODER'

    if start or end:
      mc2 = mc2 + " @@END@@ @@START@@ TIMECLIP "
      if start:
        mc2 = mc2.replace("@@START@@", str(start) + " MSTU * TOLONG ISO8601")
      else:
        mc2 = mc2.replace("@@START@@", "MINLONG ISO8601")

      if end:
        mc2 = mc2.replace("@@END@@", str(end) + " MSTU * TOLONG ISO8601")
      else:
        mc2 = mc2.replace("@@END@@", "MAXLONG ISO8601")

      mc2 = mc2 + " DUP SIZE 0 == <% DROP NULL %> <% WRAPRAW %> IFTE"

      mc2 = mc2.replace('\n',' ').replace('\'','\\\'').replace('\"','\\\"')

      DF = 'DF' + str(int(time.time() * 1000000.0))
      df.createOrReplaceTempView(DF)
      df = df.sql_ctx.sql("SELECT pywarp_hfileread('" + mc2 + "', _2) AS _2 FROM " + DF)
      df = df.na.drop()

  df = df.select(col('_2').alias('wrapper'))

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

  df = df.selectExpr('`gts.class` as class', '`gts.labels` as labels', '`gts.attributes` as attributes', 'datapoint.ts as ts', 'datapoint.lat as lat', 'datapoint.lon as lon', 'datapoint.elev as elev', 'datapoint.l_value as l_value', 'datapoint.d_value as d_value', 'datapoint.b_value as b_value', 'datapoint.s_value as s_value','datapoint.bin_value as bin_value')

  return df
