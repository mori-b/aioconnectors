# aioconnectors
Simple secure asynchronous persistent message broker (Unix-like)

## FEATURES

aioconnectors is an easy to set up broker that works on Unix like systems, using Python >= 3.6.
It is built on the client/server model but both peers can push messages, it provides optional authentication and encryption, transfer of messages (string and binary) and of files, persistence in case of connection loss. It is asynchronous, provides the option to wait for response, and to wait for ack.
It comes with a command line tool that enables to easily run a connector, and manage it.
It provides a simple programmatic API, with simple functionalities like starting/stopping a connector, sending a message, or receiving messages.


## HIGH LEVEL DESIGN

The client and server are connected by one single tcp client/server socket.
When a peer sends a message, it is first sent to a unix socket, then transferred to a different queue for each remote peer. Messages are read from these queues and sent to the remote peer on the client/server socket. After a message reaches its peer, it is sent to a queue, one queue per message type. The user can chose to listen on a unix socket to receive messages of a specific type, that are read from the corresponding queue.
The optional encryption uses TLS. The server certificate is predefined, as well as the default client certificate. So that a server and client without prior knowledge of these certificates cannot interfere. Then, the server generates on the fly a new certificate per client, so that different clients cannot interfere with one another.

## USAGE

aioconnectors provides the ConnectorManager class which runs the connectors, and the ConnectorAPI class which sends and receives messages. It also provides a command line tool accessible by typing

    python3 -m aioconnectors --help

### 1.If you choose to use encryption, you should call

    python3 -m aioconnectors create_certificates <optional_directory_path>

If you don't specify optional\_directory\_path, certificates will be created in cwd.
This results in the creation of 2 directories : certificates/server and certificates/client.
All you have to do is copy certificates/server to your server, and certificates/client to your client.

### 2.You have 2 options to run your connectors, either through the command line tool, or programmatically.  
2.1.Command line tool  
-To configure the Connector Manager, create a <config\_json\_path> file based on the Manager template json.
Relevant for both server and client.

    python3 -m aioconnectors print_config_templates

-Then create you connector (both server and client)

    python3 -m aioconnectors create_connector <config_json_path>

2.2.Programmatically, examples are provided in aioconnectors\_test.py and in \_\_main\_\_.py  
to create and start a connector :

    connector_manager = aioconnectors.ConnectorManager(config_file_path=config_file_path)
    task_manager = loop.create_task(connector_manager.start_connector())

to stop a connector :

    await connector_manager.stop_connector()

to shutdown a connector :

    await connector_manager.stop_connector(shutdown=True)

You don't have to use a config file (config\_file\_path), you can also directly initialize your ConnectorManager kwargs, as shown in aioconnectors\_test.py

### 3.send/receive messages with the API

3.1.To configure the Connector API, create a <config\_json\_path> file based on the API template json.
Relevant for both server and client.

    python3 -m aioconnectors print_config_templates
    connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)

3.2.Or you can directly initialize your ConnectorAPI kwargs  

Then you can send and receive messages by calling the following coroutines in your program, as shown in aioconnectors\_test.py, and in \_\_main\_\_.py (test\_receive\_messages and test\_send\_messages)  
To send messages : 

    await connector_api.send_message(**kwargs)

This returns a status (True or False).
If you set the await\_response kwarg to True, this returns the response : a (transport\_json , data, binary) triplet

To register to receive messages of a specific type : 

    loop.create_task(connector_api.start_waiting_for_messages(message_type='', message_received_cb=message_received_cb))

message\_received\_cb is a coroutine that you must provide, receiving and processing the message triplet (transport\_json, data, binary).  
transport\_json is a json with keys related to the "transport layer" of our message protocol : source\_id, destination\_id, request\_id, response\_id, etc


### 4.More details about the ConnectorManager and ConnectorAPI arguments.

    logger=None, use_default_logger=True, default_logger_log_level='INFO', config_file_path=<path>

config\_file\_path can be the path of a json file like the following, or instead you can load its items as kwargs, as shown in aioconnectors\_test.py  
You can use both kwargs and config_file_path : if there are shared items, the ones from config_file_path will override the kwargs.  

Here is an example of config\_file\_path, with ConnectorManager class arguments, used to create a connector

    {
    "certificates_directory_path": null,
    "client_name": null,
    "connector_files_dirpath": "/tmp/aioconnectors",
    "debug_msg_counts": true,
    "default_logger_dirpath": "/tmp/aioconnectors",
    "default_logger_log_level": "INFO",
    "disk_persistence_send": true,
    "disk_persistence_recv": ["any"],
    "file_type2dirpath": {},
    "is_server": false,
    "send_message_types": [
        "any"
    ],
    "max_size_persistence_path": 1000000000,
    "recv_message_types": [
        "any"
    ],
    "server_sockaddr": [
        "127.0.0.1",
        12345
    ],
    "silent": true,
    "uds_path_receive_preserve_socket": true,
    "uds_path_send_preserve_socket": true,
    "use_ssl": true
    }

Here is an example of config\_file\_path, with ConnectorAPI class arguments, used to send/receive messages.  
These are a subset of ConnectorManager arguments : which means you can use the ConnectorManager config file also for ConnectorAPI.

    {
    "client_name": null,
    "connector_files_dirpath": "/tmp/aioconnectors",
    "default_logger_dirpath": "/tmp/aioconnectors",
    "default_logger_log_level": "INFO",
    "is_server": false,
    "send_message_types": [
        "any"
    ],
    "recv_message_types": [
        "any"
    ],
    "server_sockaddr": [
        "127.0.0.1",
        12345
    ],
    "uds_path_receive_preserve_socket": true,
    "uds_path_send_preserve_socket": true
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
-In order to enable persistence between client and server (supported on both client and server sides), use disk\_persistence\_send=True. There will be 1 persistence file per client/server connection. You can limit the persistence files size with max\_size\_persistence\_path.  
-In order to enable persistence between the connector and a message listener (supported on both client and server sides), use disk\_persistence\_recv=True. There will be 1 persistence file per message type.  
-uds\_path\_receive\_preserve\_socket should always be True for better performance, your message\_received\_cb coroutine in start\_waiting\_for\_messages stays connected to the connector once the latter starts sending it messages.  
-uds\_path\_send\_preserve\_socket should always be True for better performance.


### 5.More details about the send\_message arguments

    send_message(message_type=None, destination_id=None, request_id=None, response_id=None,
    data=None, data_is_json=True, binary=None, await_response=False, with_file=None, wait_for_ack=False) 
    with_file can be like : {'src_path':'','dst_type':'', 'dst_name':'', 'delete':False}

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
-wait\_for\_ack is not recommended for high throughputs, since it slows down dramatically. Basic testing showed a rate of 10 messages per second instead of 1000 messages per second.


### 6.Other management command line tools supported

    python3 -m aioconnectors cli

to run several interesting commands like :   
-start/stop/restart your connectors  
-show currently connected peers  
-delete\_client\_certificates enables your server to delete a specific client certificate. delete\_client\_certificates enables your client to delete its own certificate and fallback using the default one.   
-peek\_queues to show the internal queues sizes  
-ignore\_peer\_traffic to drop incoming and outgoing traffic in order to let the queues evacuate their accumulated messages.  


### 7.Testing command line tools supported.

-To send pings to a remote connector, and print its replies. 

    python3 -m aioconnectors ping <config_json_path>

-To simulate a simple application waiting for messages, and print all received messages. Your application should not wait for incoming messages when using this testing tool.

    python3 -m aioconnectors test_receive_messages <config_json_path>

-To simulate a simple application sending dummy messages.

    python3 -m aioconnectors test_send_messages <config_json_path>





