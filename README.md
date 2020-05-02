# aioconnectors
Simple secure asynchronous persistent message broker

## FEATURES

aioconnectors is an easy to set up broker that works on Unix like systems. Requirements are : Python >= 3.6, and openssl installed.  
It is built on the client/server model but both peers can push messages.  
It provides optional authentication and encryption, transfer of messages (string and binary) and of files, persistence in case of connection loss.  
It is asynchronous, provides the option to wait asynchronously for response, or to wait for ack.  
It comes with a command line tool that enables to easily run a connector, and manage it.  
It provides a simple programmatic API, with functionalities like starting/stopping a connector, sending a message, or receiving messages.


## HIGH LEVEL DESIGN

The client and server are connected by one single tcp client/server socket.
When a peer sends a message, it is first sent to a unix socket, then transferred to a different queue for each remote peer. Messages are read from these queues and sent to the remote peer on the client/server socket. After a message reaches its peer, it is sent to a queue, one queue per message type. The user can chose to listen on a unix socket to receive messages of a specific type, that are read from the corresponding queue.
The optional encryption uses TLS. The server certificate is predefined, as well as the default client certificate. So that a server and client without prior knowledge of these certificates cannot interfere. Then, the server generates on the fly a new certificate per client, so that different clients cannot interfere with one another.


## USE CASES

-The standard use case is running a server or a client on each station. Each client station can then initiate a connection to the server station.  
In order to have all clients/server connections authenticated and encrypted, the first step is to call

    python3 -m aioconnectors create_certificates <optional_directory_path>

And then to share the created directories between server and clients as explained in 1-.  

-You might want both sides to be able to initiate a connection, or even to have multiple nodes being able to initiate connections between one another.  
The following lines describe a possible approach to do that using aioconnectors. However, reading the rest of the documentation is necessary to understand them.  
Each node should be running an aioconnector server, and be able to also spawn an aioconnector client each time it initiates a connection to a different remote server.  
Your application might need to know if a peer is already connected before initiating a connection : to do so, you might use the connector_manager.show\_connected\_peers method in aioconnectors\_core.py.  
A comfortable approach would be to share the certificates directories created in the first step between all the nodes. All nodes would share the same server certificate, and use the same client default certificate to initiate the connection (before receiving their individual certificate). The only differences between clients configurations would be their client_name, and their remote server (the configurations are explained in 4-).

## USAGE

aioconnectors provides the ConnectorManager class which runs the connectors, and the ConnectorAPI class which sends and receives messages. It also provides a command line tool accessible by typing

    python3 -m aioconnectors --help

### 1.If you choose to use encryption, you should call

    python3 -m aioconnectors create_certificates <optional_directory_path>

A directory called "certificates" will be created under your optional\_directory\_path, or under /tmp/aioconnectors if not specified.
Under it, 2 subdirectories will be created : certificates/server and certificates/client.  
The default mode is the most secure : use_ssl is enabled and ssl_allow_all is disabled.  
In such a case, you need to copy certificates/server to your server, and certificates/client to your client. That's all you have to do.  
This is the recommended approach, since it ensures traffic encryption, client and server authentication, and prevents client impersonation.  
Less secure options :  
By setting ssl_allow_all, you can use encryption without the hassle of sharing certificates. In such a case you can run independently create_certificates on server and client side, without the need to copy a directory. This disables authentication, so that any client and server can communicate.  
By unsetting use_ssl, you can disable encryption at all.


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
If you set the await\_response kwarg to True, this returns the response : a (transport\_json , data, binary) triplet.

To register to receive messages of a specific type : 

    loop.create_task(connector_api.start_waiting_for_messages(message_type='', message_received_cb=message_received_cb))

message\_received\_cb is a coroutine that you must provide, receiving and processing the message triplet (transport\_json, data, binary).  
transport\_json is a json with keys related to the "transport layer" of our message protocol : source\_id, destination\_id, request\_id, response\_id, etc.


### 4.More details about the ConnectorManager and ConnectorAPI arguments.

    logger=None, use_default_logger=True, default_logger_log_level='INFO', config_file_path=<path>

config\_file\_path can be the path of a json file like the following, or instead you can load its items as kwargs, as shown in aioconnectors\_test.py  
You can use both kwargs and config_file_path : if there are shared items, the ones from config_file_path will override the kwargs.  

Here is an example of config\_file\_path, with ConnectorManager class arguments, used to create a connector

    {
    "certificates_directory_path": null,
    "client_bind_ip": null,
    "client_name": null,
    "connector_files_dirpath": "/tmp/aioconnectors",
    "debug_msg_counts": true,
    "default_logger_dirpath": "/tmp/aioconnectors",
    "default_logger_log_level": "INFO",
    "disk_persistence_recv": true,
    "disk_persistence_send": ["any"],
    "file_type2dirpath": {},
    "is_server": true,
    "max_size_persistence_path": 1000000000,
    "recv_message_types": [
        "any"
    ],
    "send_message_types": [
        "any"
    ],
    "server_sockaddr": [
        "127.0.0.1",
        10673
    ],
    "silent": true,
    "ssl_allow_all": false,
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
    "is_server": true,
    "send_message_types": [
        "any"
    ],
    "recv_message_types": [
        "any"
    ],
    "server_sockaddr": [
        "127.0.0.1",
        10673
    ],
    "uds_path_receive_preserve_socket": true,
    "uds_path_send_preserve_socket": true
    }

-is\_server (boolean) is important to differentiate between server and client  
-server\_sockaddr can be configured as a tuple when used as a kwarg, or as a list when used in the json, and is mandatory on both server and client sides.  
-client\_name is used on client side. It is the name that will be associated with this client on server side. Auto generated if not supplied in ConnectorManager. Mandatory in ConnectorAPI.  
-client_bind_ip is optional, specifies the interface to bind your client.  
-use\_ssl and ssl_allow_all are boolean. use_ssl enables encryption as explained previously. When ssl_allow_all is disabled, certificates validation is enforced.  
-certificates\_directory\_path is where your certificates are located, if use\_ssl is True.  
-connector\_files\_dirpath is important, it is the path where all internal files are stored. The default is /tmp/aioconnectors. unix sockets files, default log files, and persistent files are stored there.  
-send\_message\_types : the list of message types that can be sent from connector. Default is ["any"] if you don't care to differentiate between message types on your application level.  
-recv\_message\_types : the list of message types that can be received by connector. Default is ["any"]. It should include the send\_message\_types using await\_response.  
-In order to be able to receive files, you must define the destination path of files according to their associated dst\_type. This is done in file\_type2dirpath, as shown in aioconnectors\_test.py  
-In order to enable persistence between client and server (supported on both client and server sides), use disk\_persistence\_send=True. There will be 1 persistence file per client/server connection. You can limit the persistence files size with max\_size\_persistence\_path.  
-In order to enable persistence between the connector and a message listener (supported on both client and server sides), use disk\_persistence\_recv=True. There will be 1 persistence file per message type.  
-uds\_path\_receive\_preserve\_socket should always be True for better performance, your message\_received\_cb coroutine in start\_waiting\_for\_messages stays connected to the connector once the latter starts sending it messages.  
-uds\_path\_send\_preserve\_socket should always be True for better performance.  
-debug_msg_counts is a boolean, enables to display every 2 minutes a count of messages in the log file, and in stdout if silent is disabled.


### 5.More details about the send\_message arguments

    send_message(message_type=None, destination_id=None, request_id=None, response_id=None,
    data=None, data_is_json=True, binary=None, await_response=False, with_file=None, wait_for_ack=False) 
    with_file can be like : {'src_path':'','dst_type':'', 'dst_name':'', 'delete':False}

These arguments must be filled on the application layer by the user  
-message\_type is mandatory, it enables to have different listeners that receive different message types. You can use "any" as a default.  
-destination\_id is mandatory for server : it is the remote client id.  
-data is the payload of your message. Usually it is a json, but it can even be binary. However you probably prefer to use a binary payload together with some text information, so best practice would be to keep "data" as a json or string, and use the "binary" argument for binary payload.  
-data\_is\_json is True by default since it assumes "data" is a json, and it dumps it automatically. Set it to False if "data" is not a json.  
-with\_file lets you embed a file, with {'src\_path':'','dst\_type':'', 'dst\_name':'', 'delete':False}. src\_path is the source path of the file to be sent, dst\_type is the type of the file, which enables the remote peer to evaluate the destination path thanks to its ConnectorManager attribute "file\_type2dirpath". dst\_name is the name the file will be stored under. "delete" is a boolean telling if to delete the source file after it has been sent.  
-request\_id and response\_id are helpful to keep track of asynchronous messages on the application layer.  
-await\_response is False by default, set it to True if your coroutine calling send\_message expects a response value.  
In such a case, the remote peer has to answer with response\_id equal to the request\_id.
This is shown in aioconnectors\_test.py.  
-wait\_for\_ack is not recommended for high throughputs, since it slows down dramatically. Basic testing showed a rate of 10 messages per second instead of 1000 messages per second.


### 6.Other management command line tools

    python3 -m aioconnectors cli

to run several interesting commands like :   
-start/stop/restart your connectors  
-show currently connected peers  
-delete\_client\_certificates enables your server to delete a specific client certificate. delete\_client\_certificates enables your client to delete its own certificate and fallback using the default one.   
-peek\_queues to show the internal queues sizes  
-ignore\_peer\_traffic to drop incoming and outgoing traffic in order to let the queues evacuate their accumulated messages.  


### 7.Testing command line tools

-To let your connector send pings to a remote connector, and print its replies. 

    python3 -m aioconnectors ping <config_json_path>

-To simulate a simple application waiting for messages, and print all received messages. Your application should not wait for incoming messages when using this testing tool.

    python3 -m aioconnectors test_receive_messages <config_json_path>

-To simulate a simple application sending dummy messages.

    python3 -m aioconnectors test_send_messages <config_json_path>


### 8.Funny embedded chat

As simple chat using aioconnectors is embedded, it is encrypted, and allows you to exchange messages, files and directories easily between 2 Linux stations.  
It is not a multi user chat, but more of a tool to easily transfer stuff between your computers. Warning : the server does not perform client authentication.

-On the 1st station (server side), type : 

    python3 -m aioconnectors chat

-Then on the 2nd station (client side), type :

    python3 -m aioconnectors chat --target <server_ip> 

You can execute local shell commands by preceding them with a \"\!\".  
You can also upload files during a chat, by typing \"\!upload \<file or dir path\>\".  
Files are uploaded to /tmp/aioconnectors. A directory is transferred as a zip file.  
When receiving a file, you can automatically copy it from /tmp/aioconnectors to your current working directory by typing \"\!import \<file name\>\".  
You can also additionaly unzip it by using instead \"\!zimport \<file name\>\".  

-On client side, you can also directly upload a file or directory to the server without opening a chat :

     python3 -m aioconnectors chat --target <server_ip> --upload <file or dir path>


-More info :

    python3 -m aioconnectors chat --help

-If you need your server to listen on a specific interface :

    python3 -m aioconnectors chat bind_server_ip <server_ip>
    
-If you don't want your server to use the default port (10673) : 

    python3 -m aioconnectors chat --port <port> [--target <server_ip>]


## Windows ?

To port aioconnectors to Windows, these steps should be taken, and probably more :  
-Replace usage of unix sockets by local sockets (for example).  
Since the implementation relies on unix sockets paths, a possible approach would be to preserve these paths, and manage a mapping between the paths and their corresponding local listening ports.  
-Port the usage of openssl in ssl_helper.py  
-Convert paths format


## BASIC EXAMPLE

This is a basic example of a server and a client sending messages to each other. For more interesting examples, please refer to aioconnectors_test.py or \_\_main\_\_.py.  
First step is to run : 

    python3 -m aioconnectors create_certificates

In order not to enable allow_ssl_all, you should copy /tmp/aioconnectors/certificates/server to your server and /tmp/aioconnectors/certificates/client to your client.  
For both server and client, connector_manager is running the connector, and connector_api is sending/receiving messages. In this example, connector_manager and connector_api are running in the same process for convenience. They can obviously run in different processes, as shown in the other examples.

### Server example

    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/tmp/aioconnectors'
    connector_manager = aioconnectors.ConnectorManager(is_server=True, server_sockaddr=server_sockaddr, use_ssl=True, ssl_allow_all=True,
                                                       connector_files_dirpath=connector_files_dirpath, certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'], file_type2dirpath={'any':connector_files_dirpath})
                
    connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=server_sockaddr, connector_files_dirpath=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'], default_logger_log_level='INFO')

    loop.create_task(connector_manager.start_connector())

    async def message_received_cb(transport_json , data, binary):
        print('SERVER : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))

    async def send_messages(destination):
        index = 0
        while True:
            index += 1
            await connector_api.send_message(data={'application message': f'SERVER MESSAGE {index}'}, message_type='any', destination_id=destination)
            await asyncio.sleep(1)
                                                    
    loop.create_task(send_messages(destination='client1'))

    try:
        loop.run_forever()
    except:
        print('Connector stopped !')

    connector_api.stop_waiting_for_messages(message_type='any')
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)

### Client example

    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/tmp/aioconnectors'
    client_name = 'client1'
    connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr, use_ssl=True, ssl_allow_all=True,
                                                       connector_files_dirpath=connector_files_dirpath, certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'], file_type2dirpath={'any':connector_files_dirpath}, client_name=client_name)

    connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr, connector_files_dirpath=connector_files_dirpath, client_name=client_name,
                                                       send_message_types=['any'], recv_message_types=['any'], default_logger_log_level='INFO')


    loop.create_task(connector_manager.start_connector())

    async def message_received_cb(transport_json , data, binary):
        print('CLIENT : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))

    async def send_messages():
        index = 0
        while True:
            index += 1
            await connector_api.send_message(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='any')
            await asyncio.sleep(1)
                                           
    loop.create_task(send_messages())

    try:
        loop.run_forever()
    except:
        print('Connector stopped !')

    connector_api.stop_waiting_for_messages(message_type='any')
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)


