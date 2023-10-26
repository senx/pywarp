#!/usr/bin/env python3
#
#   Copyright 2023  SenX S.A.S.
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

import pandas

from .splitfetch import sfetch

from .fetch import _streamFetchResponse, _parseLine

TIMESTAMP = 'ts'
LATITUDE = 'lat'
LONGITUDE = 'lon'
ELEVATION = 'elev'
VALUE = 'value'
CLASSNAME = 'warp10.classname'
LABELS = 'warp10.labels'
ATTRIBUTES = 'warp10.attributes'

def ffetch(endpoint, token, selector, end, timespan, indexedByTimestamp=False):
  """
Read data from a Warp 10 instance using the specified /api/v0/fetch endpoint.
Outputs a single amalgamated pandas dataframe.
  """

  headers = {}
  headers['X-Warp10-Token'] = token
  params = {}
  params['selector'] = selector
  params['end'] = str(end)
  params['timespan'] = str(timespan)
  params['showattr'] = 'true'
  params['format'] = 'text'

  #
  # We will create the dataframe using from_records method
  # data is passed as a list of dict. Each dict represents a row.
  # for each new data point from the stream, we need to check if there is a row where it can be inserted (adding only one col)
  # it must have the same ts, label set and attribute set to be inserted in this row
  # if not we add a new row
  #

  datadict = {} # key is an ident made of ts/labels/attributes, and value is the row dict
  for line in _streamFetchResponse(endpoint, headers, params):
    (ts,lat,lon,elev,cls,lbls,attributes,value) = _parseLine(line)
    if not cls:
      cls = lastcls
      lbls = lastlbls
      attributes = lastattr
    else:
      lastcls = cls
      lastlbls = lbls
      lastattr = attributes

    ident = str(ts) + str(lbls) + str(attributes)
    if not(ident in datadict):
      datadict[ident] = {TIMESTAMP: ts}
      for k in lbls:
        datadict[ident]['l:' + k] = lbls[k]
      for k in attributes:
        datadict[ident]['a:' + k] = attributes[k]
    
    datadict[ident][VALUE + ':' + cls] = value
    if lat is not None:
      datadict[ident][LATITUDE + ':' + cls] = lat
      datadict[ident][LONGITUDE + ':' + cls] = lon
    if elev is not None:
      datadict[ident][ELEVATION + ':' + cls] = elev

  df = pandas.DataFrame.from_records(iter(datadict.values()))

  if indexedByTimestamp:
    df = df.set_index(TIMESTAMP)
  
  return df