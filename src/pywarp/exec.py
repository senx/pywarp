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

def exec(endpoint, mc2, unpickle=False):
  """
Executes WarpScript on a Warp 10 instance and return the result.
  """

  resp = requests.post(endpoint, data=mc2)
  obj = json.loads(resp.text)

  if unpickle:
    pickled = base64.b64decode(obj[0])
    obj = pickle.loads(pickled)

  return obj

