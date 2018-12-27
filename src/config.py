# config

from collections import namedtuple
from typing import List,NamedTuple
import json

# Folders:
_WORK_DIR = "/home/im/mypy/ping"
_CONFIG_DIR = f"{_WORK_DIR}/config"
_DEVICES_FNAME = "devices.json"
RESULT_DIR = f"{_WORK_DIR}/to_server"

DevInfo = namedtuple('DevInfo','name, host, seqn')
Devices : List[NamedTuple] = [DevInfo._make(d) for d in json.load(open(f"{_CONFIG_DIR}/{_DEVICES_FNAME}"))]
