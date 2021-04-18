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

aioconnectors is an easy to set up broker that currently works on Unix like systems.
Requirements are : Python >= 3.6, and openssl installed.
It makes it easy to securely send and receive loads of messages and files between remote applications.
- Easy and fast installation with pip, zero dependency
- Nice trade off between ease of use and efficiency
- Supports several use cases : transfer of messages, files, authentication, encryption, persistence, point-to-point, publish/subscribe. All configurable by simply modifying a configuration file
- User friendly and intuitive API, with simple Python asynchronous functions to send and receive messages, easy to integrate in an existing asyncio code base
- Bidirectional : client and server can push messages to each other
- Embeds a command line interface, which can manage the broker through the command line
- Embeds an encrypted chat/file transfer tool easily callable through the command line

The command line tool can be called by "python3 -m aioconnectors --help"
Usage examples can be found in applications.py
'''

__version__ = '1.0.42'
__author__ = 'Mori Benech'

from .api import ConnectorManager, ConnectorAPI, ConnectorRemoteTool
from .connection import MessageFields
from .helpers import get_logger, iface_to_ip, get_tmp_dir
from . import applications
