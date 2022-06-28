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

import time

def _streamFetchResponse(url, headers, params):
  req = requests.Request("GET",url, headers=headers, params=params).prepare()

  s = requests.Session()
  resp = s.send(req, stream=True)

  for line in resp.iter_lines():
    if line:
      yield line.decode('utf-8')

def _unquote(expr):
  # Decode percent encoding
  return unquote(expr)

def _parseValue(val):
  if val[0] == "'":
    # String
    val = _unquote(val[1:-1])
  elif val[0:4] == 'b64:':
    # Binary
    val = base64.b64decode(val[4:]) 
  elif val == 'T':
    val = True
  elif val == 'F':
    val = False
  elif '.' in val:
    # Double - handle NaN / +Infinity / -Infinity
    val = float(val)
  elif 'NaN' == val:
    val = math.nan
  elif 'Infinity' == val or '+Infinity' == val:
    val = math.inf
  elif '-Infinity' == val:
    val = -math.inf
  else:
    # Long
    val = int(val)

  return val

def _parseLine(line):
  if '=' == line[0]:
    (tsgeo,value) = line[1:].split(' ')
    (ts,latlon,elev) = tsgeo.split('/')
    cls = None
    lbls = None
    attributes = None
  else:
    # Format is TS/LAT:LON/ELEV CLASS{LABELS}{ATTR} VALUE
    # Split on ' '
    (tsgeo,meta,value) = line.split(' ')
    (ts,latlon,elev) = tsgeo.split('/')
    (cls,labels,attr) = meta.split('{')
    cls = _unquote(cls)
    labels = labels[:-1].split(',')
    lbls = {}
    for label in labels:
      if '=' in label:
        (name,val) = label.split('=')
        name = _unquote(name)
        val = _unquote(val)
        lbls[name] = val
    attr = attr[:-1].split(',')
    attributes = {}
    for att in attr:
      if '=' in att:
        (name,val) = att.split('=')
        name = _unquote(name)
        val = _unquote(val)
        attributes[name] = value

  value = _parseValue(value)

  ts = int(ts)

  if '' == elev:
    elev = None

  if '' == latlon:
    lat = None
    lon = None
  else:
    (lat,lon) = latlon.split(':')
    lat = float(lat)
    lon = float(lon)

  return (ts,lat,lon,elev,cls,lbls,attributes,value)

def fetch(endpoint, token, selector, end, timespan):
  """
Read data from a Warp 10 instance using the specified /api/v0/fetch endpoint.
Outputs a pandas dataframe.
  """

  headers = {}
  headers['X-Warp10-Token'] = token
  params = {}
  params['selector'] = selector
  params['end'] = str(end)
  params['timespan'] = str(timespan)
  params['showattr'] = 'true'
  params['format'] = 'text'
  
  df = pandas.DataFrame(columns= ['class','labels','attributes','ts','lat','lon','elev','l_value','d_value','b_value','s_value','bin_value'])

  all = [df]

  lclasses = []
  llabels = []
  lattributes = []
  lts = []
  llat = []
  llon = []
  lelev = []
  llval = []
  ldval = []
  lbval = []
  lsval = []
  lbinval = []
  
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

    lval = None
    dval = None
    bval = None
    sval = None
    binval = None

    if isinstance(value,int):
      lval = value
    elif isinstance(value,float):
      dval = value
    elif isinstance(value,bool):
      bval = value
    elif isinstance(value,str):
      sval = value
    else:
      binval = value
    
    lclasses.append(cls)
    llabels.append(lbls)
    lattributes.append(attributes)
    lts.append(ts)
    llat.append(lat)
    llon.append(lon)
    lelev.append(elev)
    llval.append(lval)
    ldval.append(dval)
    lbval.append(bval)
    lsval.append(sval)
    lbinval.append(binval)

  df = pandas.DataFrame(data = {
          'class' : lclasses,
          'labels' : llabels,
          'attributes' : lattributes,
          'ts' : lts,
          'lat' : llat,
          'lon' : llon,
          'elev' : lelev,
          'l_value' : llval,
          'd_value' : ldval,
          'b_value' : lbval,
          's_value' : lsval,
          'bin_value' : lbinval,
        }
        , columns= ['class','labels','attributes','ts','lat','lon','elev','l_value','d_value','b_value','s_value','bin_value']
        , copy = False)

  return df

if __name__ == "__main__":
  # https://manytools.org/hacker-tools/ascii-banner/ - speed
  banner = """
________        ___       __                      
___  __ \____  ___ |     / /_____ _______________ 
__  /_/ /_  / / /_ | /| / /_  __ `/_  ___/__  __ \
_  ____/_  /_/ /__ |/ |/ / / /_/ /_  /   __  /_/ /
/_/     _\__, / ____/|__/  \__,_/ /_/    _  .___/ 
        /____/                           /_/      
"""
  print(banner)
