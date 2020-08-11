'''
   Copyright 2020 Mori Benech

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''
'''
https://github.com/mori-b/aioconnectors

aioconnectors is an easy to set up broker that works on Unix like systems. Requirements are : Python >= 3.6, and openssl installed.
It provides optional authentication and encryption, transfer of messages and files, persistence in case of connection loss.
It is built on the client/server model but both peers can push messages. Based on asyncio, message sending and receiving are asynchronous,
either independent or with the option to wait asynchronously for a response.
A connector can be configured with a short json file. An embedded command line tool enables to easily run a connector
and manage it with shell commands.
A simple programmatic Python API is also exposed, with functionalities like starting/stopping a connector,
sending a message, or receiving messages, and other management capabilities.

The command line tool can be called by "python3 -m aioconnectors --help"
Usage examples can be found in applications.py
'''

__version__ = '1.0.18'
__author__ = 'Mori Benech'

from .api import ConnectorManager, ConnectorAPI, ConnectorRemoteTool
from .connection import MessageFields
from .helpers import get_logger, iface_to_ip
from . import applications
