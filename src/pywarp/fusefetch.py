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

TIMESTAMP = 'ts'
LATITUDE = 'lat'
LONGITUDE = 'lon'
ELEVATION = 'elev'
VALUE = 'value'
CLASSNAME = 'warp10.classname'
LABELS = 'warp10.labels'
ATTRIBUTES = 'warp10.attributes'

def _expandMetadata(df):
  for key in df.attrs[LABELS]:
    df[key] = [df.attrs[LABELS][key]] * len(df)
  
  for key in df.attrs[ATTRIBUTES]:
    df[key] = [df.attrs[ATTRIBUTES][key]] * len(df)

  df.rename(columns={LATITUDE: LATITUDE + ':' + df.attrs[CLASSNAME],
                     LONGITUDE: LONGITUDE + ':' + df.attrs[CLASSNAME],
                     ELEVATION: ELEVATION + ':' + df.attrs[CLASSNAME]},
            inplace=True)
  
  return df

def ffetch(endpoint, token, selector, end, timespan):
  """
Read data from a Warp 10 instance using the specified /api/v0/fetch endpoint.
Outputs a single amalgamated pandas dataframe.
  """

  dfs = sfetch(endpoint, token, selector, end, timespan, indexedByTimestamp=True)

  if 0 == len(dfs):
    return pandas.DataFrame()

  fdf = _expandMetadata(dfs[0])
  for df in dfs[1:]:
    fdf = fdf.combine_first(_expandMetadata(df))

  return fdf