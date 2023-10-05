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

from .fetch import _streamFetchResponse, _parseLine

TIMESTAMP = 'ts'
LATITUDE = 'lat'
LONGITUDE = 'lon'
ELEVATION = 'elev'
VALUE = 'value'
CLASSNAME = 'warp10classname'
LABELS = 'warp10labels'
ATTRIBUTES = 'warp10attributes'
HASLATLON = 'haslatlon'
HASELEV = 'haselev'

def splitfetch(endpoint, token, selector, end, timespan):
  """
Read data from a Warp 10 instance using the specified /api/v0/fetch endpoint.
Outputs a list of pandas dataframe.
  """

  headers = {}
  headers['X-Warp10-Token'] = token
  params = {}
  params['selector'] = selector
  params['end'] = str(end)
  params['timespan'] = str(timespan)
  params['showattr'] = 'true'
  params['format'] = 'text'
  
  res = {}
  
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

    sel = cls + str(lbls)
    if not(sel in res.keys()):
      res[sel] = {
        TIMESTAMP: [],
        LATITUDE: [],
        LONGITUDE: [],
        ELEVATION: [],
        VALUE: [],
        CLASSNAME: cls,
        LABELS: lbls,
        ATTRIBUTES: attributes,
        HASLATLON: False,
        HASELEV: False
      }
    
    res[sel][TIMESTAMP].append(ts)
    res[sel][VALUE].append(value)
    if lat is not None:
      res[sel][HASLATLON] = True
    res[sel][LATITUDE].append(lat)
    res[sel][LONGITUDE].append(lon)
    if elev is not None:
      res[sel][HASELEV] = True
    res[sel][ELEVATION].append(elev)

  dfs = []
  for sel in res:
    gtsDict = res[sel]
    
    cols = [TIMESTAMP + ':' + gtsDict[CLASSNAME]]
    data = {cols[0] : gtsDict[TIMESTAMP]}
    if gtsDict[HASLATLON]:
      cols.append(LATITUDE + ':' + gtsDict[CLASSNAME])
      cols.append(LONGITUDE + ':' + gtsDict[CLASSNAME])
      data[cols[1]] = gtsDict[LATITUDE]
      data[cols[2]] = gtsDict[LONGITUDE]
    if gtsDict[HASELEV]:
      cols.append(ELEVATION + ':' + gtsDict[CLASSNAME])
      data[cols[-1]] = gtsDict[ELEVATION]
    cols.append(VALUE + ':' + gtsDict[CLASSNAME])
    data[cols[-1]] = gtsDict[VALUE]
    
    df = pandas.DataFrame(data = data, columns= cols, copy = False)
    df.attrs[CLASSNAME] = gtsDict[CLASSNAME]
    df.attrs[LABELS] = gtsDict[LABELS]
    df.attrs[ATTRIBUTES] = gtsDict[ATTRIBUTES]

    dfs.append(df)

  return dfs
