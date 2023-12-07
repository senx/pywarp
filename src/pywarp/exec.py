#!/usr/bin/env python3
#
#   Copyright 2022 - 2023  SenX S.A.S.
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
import json
import pickle
import warnings

class WarpScriptException(Exception):

  def __init__(self, resp):
    self.resp = resp
    super().__init__(self.resp.headers['X-Warp10-Error-Message'])

def exec(endpoint, mc2, unpickle=False):
  """
Execute WarpScript on a Warp 10 instance and return the result.
  """
  target = endpoint.split('/')[-1]
  if target != 'exec':
    warnings.warn('Provided URL endpoint for sending warpscript ends with /' + target + ' rather than /exec. This is not standard for an exec endpoint.')
  

  resp = requests.post(endpoint, data=mc2)
  try:
    obj = json.loads(resp.text)

    if unpickle:
      pickled = base64.b64decode(obj[0])
      obj = pickle.loads(pickled)
  
  except:
    raise WarpScriptException(resp)

  return obj

