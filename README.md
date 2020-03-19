# aioconnectors
Simple secure asynchronous persistent message broker (unix)

FEATURES

aioconnectors is an easy to set up broker that works on Unix like systems, using Python >= 3.6.
It is built on the client/server model, provides secure authentication, optional encryption, transfer of messages (string and binary) and of files, persistence in case of connection loss. It is asynchronous, provides the option to wait for response, and to wait for ack.
It comes with a command line tool that enables to easily run a connector, and manage it.
It provides a simple programmatic API, with simple functionalities like starting/stopping a connector, sending a message, or receiving messages.


HIGH LEVEL DESIGN

The client and server are connected by one single tcp client/server socket.
When a peer sends a message, it is first sent to a unix socket, then transferred to a different queue for each remote peer. Messages are read from these queues and sent to the remote peer on the client/server socket. After a message reaches its peer, it is sent to a queue, one queue per message type. The user can chose to listen on a unix socket to receive messages of a specific type, that are read from the corresponding queue.
The optional encryption uses TLS. The server certificate is predefined, as well as the default client certificate. So that a server and client without prior knowledge of these certificates cannot interfere. Then, the server generates on the fly a new certificate per client, so that different clients cannot interfere with one another.

USAGE

aioconnectors provides the ConnectorManager class which runs the connectors, and the ConnectorAPI class which sends and receives messages. It also provides a command line tool accessible by typing

    python3 -m aioconnectors --help

###1.If you choose to use encryption, the first step is to call

    python3 -m aioconnectors create\_certificates <optional\_directory\_path>

If you don't specify optional\_directory\_path, certificates will be created in cwd.
This results in the creation of 2 directories : certificates/server and certificates/client.
All you have to do is copy certificates/server to your server, and certificates/client to your client.

###2.You have 2 options to run your connectors, either through the command line tool, or programmatically.  
2.1.command line tool  
-To configure the Connector Manager, create a <config\_json\_path> file based on the Manager template json.
Relevant for both server and client.

    python3 -m aioconnectors print\_config\_templates

-Then create you connector (both server and client)

    python3 -m aioconnectors create\_connector <config\_json\_path>

2.2.programmatically, an example is provided in aioconnectors\_test.py  
to create and start a connector :

    connector\_manager = aioconnectors.ConnectorManager(config\_file\_path=config\_file\_path)
    task\_manager = loop.create\_task(connector\_manager.start\_connector())

to stop a connector :

    await connector\_manager.connector.stop(hard=False)

to shutdown a connector :

    connector\_manager.connector.shutdown\_sync()
    del connector\_manager

You don't have to use a config file (config\_file\_path), you can also directly initialize your ConnectorManager kwargs, as shown in aioconnectors\_test.py

###3.You have 2 options to send/receive messages with the API, either through the command line tool, or programmatically.

3.1.command line tool  
To configure the Connector API, create a <config\_json\_path> file based on the API template json.
Relevant for both server and client.

    python3 -m aioconnectors print\_config\_templates

3.2.or you can directly initialize your ConnectorAPI kwargs  
Then you can send and receive messages by calling the following coroutines in your program, as shown in aioconnectors\_test.py, and in \_\_main\_\_.py (test\_receive\_messages and test\_send\_messages)
To send messages : 

    await connector\_api.send\_message()

This returns a status (True or False).
If you set the await\_response kwarg to True, this returns the response : a (transport\_json , data, binary) triplet

To register to receive messages of a specific type : 

    loop.create\_task(connector\_api.start\_waiting\_for\_messages(message\_type='', message\_received\_cb=message\_received\_cb))

message\_received\_cb is a coroutine receiving and processing the message triplet (transport\_json, data, binary).


###4.More details about the ConnectorManager and ConnectorAPI arguments.

    logger=None, use\_default\_logger=True, default\_logger\_log\_level='INFO', config\_file\_path=<path>

config\_file\_path can be the path of a json file like the following, or instead you can load its values as kwargs, as shown in aioconnectors\_test.py  

Here is an example of config\_file\_path, with ConnectorManager class arguments, used to create a connector

    {
    "certificates\_directory\_path": null,
    "client\_name": null,
    "connector\_files\_dirpath": "/tmp/aioconnectors",
    "debug\_msg\_counts": true,
    "default\_logger\_dirpath": "/tmp/aioconnectors",
    "default\_logger\_log\_level": "INFO",
    "disk\_persistence": false,
    "file\_type2dirpath": {},
    "is\_server": false,
    "send\_message\_types": [
        "any"
    ],
    "max\_size\_persistence\_path": 1000000000,
    "recv\_message\_types": [
        "any"
    ],
    "server\_sockaddr": [
        "127.0.0.1",
        12345
    ],
    "silent": true,
    "uds\_path\_receive\_preserve\_socket": true,
    "uds\_path\_send\_preserve\_socket": true,
    "use\_ssl": true
    }

Here is an example of config\_file\_path, with ConnectorAPI class arguments, used to send/receive messages.  
These are a subset of ConnectorManager arguments

    {
    "client\_name": null,
    "connector\_files\_dirpath": "/tmp/aioconnectors",
    "default\_logger\_dirpath": "/tmp/aioconnectors",
    "default\_logger\_log\_level": "INFO",
    "is\_server": false,
    "send\_message\_types": [
        "any"
    ],
    "recv\_message\_types": [
        "any"
    ],
    "server\_sockaddr": [
        "127.0.0.1",
        12345
    ],
    "uds\_path\_receive\_preserve\_socket": true,
    "uds\_path\_send\_preserve\_socket": true
    }

-is\_server (boolean) is important to differentiate between server and client  
-server\_sockaddr can be configured as a tuple when used as a kwarg, or as a list when used in the json, and is mandatory on both server and client sides.  
-client\_name is mandatory on client side. It is the name that will be associated with this client on server side.  
-use\_ssl is a boolean  
-certificates\_directory\_path is where your certificates are located, if use\_ssl is True  
-connector\_files\_dirpath is important, it is the path where all internal files are stored. The default is /tmp/aioconnectors. unix sockets files, default log files, and persistent files are stored there.  
-send\_message\_types : the list of message types that can be sent from connector. Default is ["any"] if you don't care to differentiate between message types on your application level.  
-recv\_message\_types : the list of message types that can be received by connector. Default is ["any"]. It should include the send\_message\_types using await\_response.  
-In order to be able to receive files, you must define the destination path of files according to their associated dst\_type. This is done in file\_type2dirpath, as shown in aioconnectors\_test.py  
-In order to enable persistence (supported on both client and server sides), use disk\_persistence=True. There will be 1 persistence file per client/server connection. You can limit the persistence files size with max\_size\_persistence\_path.  
-uds\_path\_receive\_preserve\_socket should always be True for better performance, your message\_received\_cb coroutine in start\_waiting\_for\_messages stays connected to the connector once the latter starts sending it messages.  
-uds\_path\_send\_preserve\_socket should always be True for better performance.


###5.More details about the send\_message arguments

    send\_message(message\_type=None, destination\_id=None, request\_id=None, response\_id=None,
    data=None, data\_is\_json=True, binary=None, await\_response=False, with\_file=None, wait\_for\_ack=False) 
    with\_file can be like : {'src\_path':'','dst\_type':'', 'dst\_name':'', 'delete':False}

These arguments must be filled on the application layer by the user  
-message\_type is mandatory, it enables to have different listeners that receive different message types. You can use "any" as a default.  
-destination\_id is mandatory for server : it is the remote client id  
-data is the payload of your message. Usually it is a json, but it can even be binary. However you probably prefer to use a binary payload together with some text information, so best practice would be to keep "data" as a json or string, and use the "binary" argument for binary payload.  
-data\_is\_json is True by default since it assumes "data" is a json, and it dumps it automatically. Set it to False if "data" is not a json.  
-with\_file lets you embed a file, with {'src\_path':'','dst\_type':'', 'dst\_name':'', 'delete':False}. src\_path is the source path of the file to be sent, dst\_type is the type of the file, which enables the remote peer to evaluate the destination path thanks to its ConnectorManager attribute "file\_type2dirpath". dst\_name is the name the file will be stored under. "delete" is a boolean telling if to delete the source file after it has been sent.  
-request\_id and response\_id are helpful to keep track of asynchronous messages on the application layer  
-await\_response is False by default, set it to True if your coroutine calling send\_message expects a response value.  
In such a case, the remote peer has to answer with response\_id equal to the request\_id.
This is shown in aioconnectors\_test.py.  
-wait\_for\_ack is not recommended for high throughputs, since it slows down dramatically. Basic testing showed a rate of 10 messages per second instead of 1500 messages per second.


###6.Other management command line tools supported

    python3 -m aioconnectors cli

to run several interesting commands like :   
-start/stop/restart your connectors  
-show currently connected peers  
-delete\_client\_certificates enables your server to delete a specific client certificate. delete\_client\_certificates enables your client to delete its own certificate and fallback using the default one.   
-peek\_queues to show the internal queues sizes  
-ignore\_peer\_traffic to drop incoming and outgoing traffic in order to let the queues evacuate their accumulated messages.  


###7.Testing command line tools supported.

-To sends pings to a remote connector, and prints its replies. 

    python3 -m aioconnectors ping <config\_json\_path>

-To simulate a simple application waiting for messages, and prints all received messages. Your application should not wait for incoming messages when using this testing tool.

    python3 -m aioconnectors test\_receive\_messages <config\_json\_path>

-To simulate a simple application sending dummy messages.

    python3 -m aioconnectors test\_send\_messages <config\_json\_path>





