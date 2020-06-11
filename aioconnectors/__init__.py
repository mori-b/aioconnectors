'''
MIT License

Copyright (c) 2020 Mori Benech

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
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

__version__ = '1.0.0'
__author__ = 'Mori Benech'

from .connectors_api import ConnectorManager, ConnectorAPI, ConnectorRemoteTool
from .connectors_core import MessageFields
from . import applications
