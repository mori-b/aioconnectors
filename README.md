[![PyPI version](https://badge.fury.io/py/aioconnectors.svg)](https://badge.fury.io/py/aioconnectors) [![Downloads](https://static.pepy.tech/personalized-badge/aioconnectors?period=total&units=international_system&left_color=grey&right_color=blue&left_text=downloads)](https://pepy.tech/project/aioconnectors)

# aioconnectors
**Simple secure asynchronous message broker**

*<a href="#features">Features</a>*  
*<a href="#installation">Installation</a>*  
*<a href="#exampleptp">Example Point to point : Server and Client</a>*  
*<a href="#exampleps">Example publish/subscribe : Broker, Subscriber, and Publisher</a>*  
*<a href="#hld">High Level Design</a>*  
*<a href="#usecases">Use Cases</a>*  
*<a href="#usage">Usage</a>*  
*<a href="#enc">1.Encryption</a>*  
*<a href="#run">2.Run a connector</a>*  
*<a href="#sendreceive">3.Send/receive messages</a>*  
*<a href="#classes">4.ConnectorManager and ConnectorAPI</a>*  
*<a href="#send">5.send_message</a>*  
*<a href="#management">6.Programmatic management tools</a>*  
*<a href="#cli">7.Command line interface management tools</a>*  
*<a href="#testing">8.Testing tools</a>*  
*<a href="#chat">9.Embedded chat</a>*  
*<a href="#containers">Containers</a>*  
*<a href="#windows">Windows</a>*  


<a name="features"></a>
## FEATURES

aioconnectors is an easy to set up message broker that works on Unix like systems. Requirements are : Python >= 3.6, and openssl installed.  
It provides transfer of messages and files, optional authentication and encryption, persistence in case of connection loss.  
It is a point to point broker built on the client/server model, but both peers can push messages. It can also be easily configured as a publish/subscribe broker.  
Based on asyncio, message sending and receiving are asynchronous, either independent or with the option to wait asynchronously for a response.  
A connector can be configured with a short json file.  
An embedded command line tool enables to easily run a connector and manage it with shell commands.  
A simple programmatic Python API is also exposed, with functionalities like starting/stopping a connector, sending a message, or receiving messages, and other management capabilities. To support other languages for the API, the file standalone\_api.py only should be translated.


<a name="installation"></a>
## INSTALLATION

    pip3 install aioconnectors


<a name="exampleptp"></a>
## BASIC EXAMPLE - POINT TO POINT

You can run a connector with a single shell command 

    python3 -m aioconnectors create_connector <config_json_path>

This is covered in <a href="#run">2-</a>, but this example shows the programmatic way to run connectors.  
This is a basic example of a server and a client sending messages to each other. For more interesting examples, please refer to applications.py or aioconnectors\_test.py.  
For both server and client, connector\_manager is running the connector, and connector\_api is sending/receiving messages.  
In this example, connector\_manager and connector\_api are running in the same process for convenience. They can obviously run in different processes, as shown in the other examples.  
In this example we are running server and client on the same machine since server_sockaddr is set to "127.0.0.1".  
To run server and client on different machines, you should modify server_sockaddr value in both server and client code, with the ip address of the server.  
You can run multiple clients, just set a different client\_name for each client.  

<u>1.No encryption</u>  
You can run the following example code directly, the encryption is disabled.  
In case you want to use this example with encryption, you should read 2. and 3., otherwise you can skip to the code.  

<u>2.Encryption without authentication</u>  
In order to use encryption, you should set use\_ssl to True in both server and client ConnectorManager instantiations.  
A directory containing certificates must be created before running the example, which is done by a single command :

    python3 -m aioconnectors create_certificates

If you run server and client on different machines, this command should be run on both machines.  

<u>3.Encryption with authentication</u>  
In this example, the kwarg ssl\_allow\_all is enabled (both on server and client), meaning the communication between server and client if encrypted is not authenticated. In case you want to run this example with authentication too, you should set use\_ssl to True and ssl\_allow\_all to False in both server and client ConnectorManager instantiations.  
If you run server and client on the same machine, this only requires to run the command "python3 -m aioconnectors create\_certificates" beforehand like in 2.  
In case the server and client run on different machines, you should run the prerequisite command "python3 -m aioconnectors create_certificates" only once, and copy the generated directory /var/tmp/aioconnectors/certificates/server to your server (preserving symlinks) and /var/tmp/aioconnectors/certificates/client to your client.  


### Server example

    import asyncio
    import aioconnectors
    
    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/var/tmp/aioconnectors'
    
    #create connector
    connector_manager = aioconnectors.ConnectorManager(is_server=True, server_sockaddr=server_sockaddr, use_ssl=False,
                                                       ssl_allow_all=True, connector_files_dirpath=connector_files_dirpath,
                                                       certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'],
                                                       file_recv_config={'any': {'target_directory':connector_files_dirpath}},
                                                       reuse_server_sockaddr=True)
    
    task_manager =  loop.create_task(connector_manager.start_connector())
    loop.run_until_complete(task_manager)
    
    #create api
    connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=server_sockaddr,
                                               connector_files_dirpath=connector_files_dirpath,
                                               send_message_types=['any'], recv_message_types=['any'],
                                               default_logger_log_level='INFO')
    
    #start receiving messages
    async def message_received_cb(logger, transport_json , data, binary):
        print('SERVER : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))
    
    #start sending messages
    async def send_messages(destination):
        await asyncio.sleep(2)
        index = 0
        while True:
            index += 1
            await connector_api.send_message(data={'application message': f'SERVER MESSAGE {index}'},
                                             message_type='any', destination_id=destination)
            await asyncio.sleep(1)
                                                    
    loop.create_task(send_messages(destination='client1'))
    
    try:
        print(f'Connector is running, check log at {connector_files_dirpath+"/aioconnectors.log"}'
              f', type Ctrl+C to stop')
        loop.run_forever()
    except:
        print('Connector stopped !')
    
    #stop receiving messages
    connector_api.stop_waiting_for_messages(message_type='any')
    
    #stop connector
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)


### Client example

    import asyncio
    import aioconnectors
    
    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/var/tmp/aioconnectors'
    client_name = 'client1'
    
    #create connector
    connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr,
                                                       use_ssl=False, ssl_allow_all=True,
                                                       connector_files_dirpath=connector_files_dirpath,
                                                       certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'],
                                                       file_recv_config={'any': {'target_directory':connector_files_dirpath}},
                                                       client_name=client_name)
    
    task_manager =  loop.create_task(connector_manager.start_connector())
    loop.run_until_complete(task_manager)
    
    #create api
    connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr,
                                               connector_files_dirpath=connector_files_dirpath, client_name=client_name,
                                               send_message_types=['any'], recv_message_types=['any'],
                                               default_logger_log_level='INFO')
    
    #start receiving messages
    async def message_received_cb(logger, transport_json , data, binary):
        print('CLIENT : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))
    
    #start sending messages
    async def send_messages():
        await asyncio.sleep(1)
        index = 0
        while True:
            index += 1
            await connector_api.send_message(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='any')
            await asyncio.sleep(1)
                                           
    loop.create_task(send_messages())
    
    try:
        print(f'Connector is running, check log at {connector_files_dirpath+"/aioconnectors.log"}'
              f', type Ctrl+C to stop')
        loop.run_forever()
    except:
        print('Connector stopped !')
    
    #stop receiving messages
    connector_api.stop_waiting_for_messages(message_type='any')
    
    #stop connector
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)


<a name="exampleps"></a>
## BASIC EXAMPLE - PUBLISH/SUBSCRIBE

You can run the following code of a broker, a publisher and a subscriber in 3 different shells on the same machine out of the box.  
You should modify some values as explained in the previous example in order to run on different machines, and with encryption.  

### Broker example

Just a server with pubsub\_central\_broker=True

    import asyncio
    import aioconnectors

    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/var/tmp/aioconnectors'

    #create connector
    connector_manager = aioconnectors.ConnectorManager(is_server=True, server_sockaddr=server_sockaddr, use_ssl=False,
                                                       ssl_allow_all=True, connector_files_dirpath=connector_files_dirpath,
                                                       certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['any'],
                                                       file_recv_config={'any': {'target_directory':connector_files_dirpath}},
                                                       pubsub_central_broker=True, reuse_server_sockaddr=True)

    task_manager =  loop.create_task(connector_manager.start_connector())
    loop.run_until_complete(task_manager)

    #create api
    connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=server_sockaddr,
                                               connector_files_dirpath=connector_files_dirpath,
                                               send_message_types=['any'], recv_message_types=['any'],
                                               default_logger_log_level='INFO')

    #start receiving messages
    async def message_received_cb(logger, transport_json , data, binary):
        print('SERVER : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))

    try:
        print(f'Connector is running, check log at {connector_files_dirpath+"/aioconnectors.log"}'
              f', type Ctrl+C to stop')
        loop.run_forever()
    except:
        print('Connector stopped !')

    #stop receiving messages
    connector_api.stop_waiting_for_messages(message_type='any')

    #stop connector
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)


### Subscriber example

Just a client with subscribe\_message\_types = [topic1, topic2, ...]

    import asyncio
    import aioconnectors

    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/var/tmp/aioconnectors'
    client_name = 'client2'

    #create connector
    connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr,
                                                       use_ssl=False, ssl_allow_all=True,
                                                       connector_files_dirpath=connector_files_dirpath,
                                                       certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['any'], recv_message_types=['type1'],
                                                       file_recv_config={'type1': {'target_directory':connector_files_dirpath}},
                                                       client_name=client_name, subscribe_message_types=["type1"])

    task_manager =  loop.create_task(connector_manager.start_connector())
    loop.run_until_complete(task_manager)

    #create api
    connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr,
                                               connector_files_dirpath=connector_files_dirpath, client_name=client_name,
                                               send_message_types=['any'], recv_message_types=['type1'],
                                               default_logger_log_level='INFO')

    #start receiving messages
    async def message_received_cb(logger, transport_json , data, binary):
        print('CLIENT : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='type1', message_received_cb=message_received_cb))

    '''
    #start sending messages
    async def send_messages():
        await asyncio.sleep(1)
        index = 0
        while True:
            index += 1
            await connector_api.send_message(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='any')
            await asyncio.sleep(1)
                                           
    loop.create_task(send_messages())
    '''

    try:
        print(f'Connector is running, check log at {connector_files_dirpath+"/aioconnectors.log"}'
              f', type Ctrl+C to stop')
        loop.run_forever()
    except:
        print('Connector stopped !')

    #stop receiving messages
    connector_api.stop_waiting_for_messages(message_type='type1')

    #stop connector
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)


### Publisher example

Just a client which uses publish\_message instead of send\_message

    import asyncio
    import aioconnectors

    loop = asyncio.get_event_loop()
    server_sockaddr = ('127.0.0.1',10673)
    connector_files_dirpath = '/var/tmp/aioconnectors'
    client_name = 'client1'

    #create connector
    connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr,
                                                       use_ssl=False, ssl_allow_all=True,
                                                       connector_files_dirpath=connector_files_dirpath,
                                                       certificates_directory_path=connector_files_dirpath,
                                                       send_message_types=['type1','type2'], recv_message_types=['any'],
                                                       file_recv_config={'any': {'target_directory':connector_files_dirpath}},
                                                       client_name=client_name, disk_persistence_send=True)

    task_manager =  loop.create_task(connector_manager.start_connector())
    loop.run_until_complete(task_manager)

    #create api
    connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr,
                                               connector_files_dirpath=connector_files_dirpath, client_name=client_name,
                                               send_message_types=['type1','type2'], recv_message_types=['any'],
                                               default_logger_log_level='INFO')

    #start receiving messages
    async def message_received_cb(logger, transport_json , data, binary):
        print('CLIENT : message received', transport_json , data.decode())
    loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))

    #start sending messages
    async def send_messages():
        await asyncio.sleep(1)
        index = 0
        #with_file={'src_path':'file_test','dst_type':'any', 'dst_name':'file_dest', 
        #           'delete':False, 'owner':'nobody:nogroup'}                  
        while True:
            index += 1
            #connector_api.publish_message_sync(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='type1')#,        
            await connector_api.publish_message(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='type1')#,
                                                #with_file=with_file, binary=b'\x01\x02\x03')
            #await connector_api.publish_message(data={'application message': f'CLIENT MESSAGE {index}'}, message_type='type2')#,                                            
            await asyncio.sleep(1)
                                           
    loop.create_task(send_messages())

    try:
        print(f'Connector is running, check log at {connector_files_dirpath+"/aioconnectors.log"}'
              f', type Ctrl+C to stop')
        loop.run_forever()
    except:
        print('Connector stopped !')

    #stop receiving messages
    connector_api.stop_waiting_for_messages(message_type='any')

    #stop connector
    task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))
    loop.run_until_complete(task_stop)



<a name="hld"></a>
## HIGH LEVEL DESIGN

The client and server are connected by one single tcp socket.
When a peer sends a message, it is first sent to a unix socket, then transferred to a different queue for each remote peer. Messages are read from these queues and sent to the remote peer on the client/server socket. After a message reaches its peer, it is sent to a queue, one queue per message type. The user can choose to listen on a unix socket to receive messages of a specific type, that are read from the corresponding queue.  
The optional encryption uses TLS. The server certificate and the default client certificate are automatically generated and pre-shared, so that a server or client without prior knowledge of these certificates cannot communicate. Then, the server generates on the fly a new certificate per client, so that different clients cannot interfere with one another.


<a name="usecases"></a>
## USE CASES

-The standard use case is running server and client on separate stations. Each client station can then initiate a connection to the server station.  
The valid message topics are defined in the server and client configurations (send\_message\_types and recv\_message\_types), and the messages are sent point to point.  
In order to have all clients/server connections authenticated and encrypted, you just have to call

    python3 -m aioconnectors create_certificates <optional_directory_path>

And then share the created directories between server and clients as explained in <a href="#enc">1-</a>.  
You can also use a proxy between your client and server, as explained in <a href="#classes">4-</a>.  

-You might prefer to use a publish/subscribe approach.  
This is also supported by configuring a single server as the broker (you just need to set pubsub\_central\_broker=True).  
The other connectors should be clients. A client can subscribe to specific topics (message\_types) by setting the attribute subscribe\_message\_types in its constructor, or by calling the set\_subscribe\_message\_types command on the fly.  

-You might want both sides to be able to initiate a connection, or even to have multiple nodes being able to initiate connections between one another.  
The following lines describe a possible approach to do that using aioconnectors.  
Each node should be running an aioconnector server, and be able to also spawn an aioconnector client each time it initiates a connection to a different remote server. A new application layer handling these connectors could be created, and run on each node.  
Your application might need to know if a peer is already connected before initiating a connection : to do so, you might use the connector_manager.show\_connected\_peers method (explained in <a href="#cli">7-</a>).  
Your application might need to be able to disconnect a specific client on the server : to do so, you might use the connector\_manager.disconnect\_client method.  
Your application might need to decide whether to accept a client connection : to do so, you might implement a hook\_server\_auth\_client method and provide it to your ConnectorManager constructor (explained in <a href="#classes">4-</a>).  
A comfortable approach would be to share the certificates directories created in the first step between all the nodes. All nodes would share the same server certificate, and use the same client default certificate to initiate the connection (before receiving their individual certificate). The only differences between clients configurations would be their client_name, and their remote server (the configurations are explained in <a href="#classes">4-</a>).  


<a name="usage"></a>
## USAGE

aioconnectors provides the ConnectorManager class which runs the connectors, and the ConnectorAPI class which sends and receives messages. It provides as well the ConnectorRemoteTool class which can lightly manage the connector outside of the ConnectorManager.  
The ConnectorManager client and server can run on different machines. However, ConnectorAPI and ConnectorRemoteTool communicate internally with their ConnectorManager, and the three must run on the same machine.  
aioconnectors also provides a command line tool accessible by typing

    python3 -m aioconnectors --help


<a name="enc"></a>
### 1.Encryption

-If you choose to use encryption, you should call

    python3 -m aioconnectors create_certificates <optional_directory_path>

A directory called "certificates" will be created under your optional\_directory\_path, or under /var/tmp/aioconnectors if not specified.
Under it, 2 subdirectories will be created : certificates/server and certificates/client.  
Encryption mode is, as everything else, configurable through the ConnectorManager kwargs or config file, as explained later in <a href="#classes">4-</a>. The relevant parameters are use_ssl and ssl_allow_all.  
The default mode is the most secure : use_ssl is enabled and ssl\_allow\_all is disabled, both on server and client.  
In such a case, you need to copy certificates/server to your server (preserving symlinks), and certificates/client to your client. That's all you have to do.  
This is the recommended approach, since it ensures traffic encryption, client and server authentication, and prevents client impersonation.  
Clients use the default certificate to first connect to server, then an individual certificate is generated by the server for each client. Client automatically uses this individual certificate for further connections. This individual certificate is mapped to the client_name.  
The first client named client_name reaching the server is granted a certificate for this client_name. Different clients further attempting to use the same client_name will be rejected.  
You can always delete a client certificate on the server (and also on client) by calling delete\_client\_certificate in

    python3 -m aioconnectors cli

For this purpose, you can also call programmatically the ConnectorManager.delete_client\_certificate method.  
You shouldn't need to modify the certificates, however there is a way to tweak the certificates template : run create\_certificates once, then modify certificates/server/csr\_details\_template.conf according to your needs (without setting the Organization field), delete other directories under certificates and run create\_certificates again.  
-Less secure options :  
By setting ssl\_allow\_all on both sever and client, you can use encryption without the hassle of sharing certificates. In such a case you can run independently create_certificates on server and client side, without the need to copy a directory. This disables authentication, so that any client and server can communicate.  
By unsetting use_ssl, you can disable encryption at all.


<a name="run"></a>
### 2.You have 2 options to run your connectors, either through the command line tool, or programmatically.

2.1.Command line tool  
-To configure the Connector Manager, create a <config\_json\_path> file based on the Manager template json, and configure it according to your needs (more details in <a href="#classes">4-</a>). Relevant for both server and client.  
A Manager template json can be obtained by calling : 

    python3 -m aioconnectors print_config_templates

-Then create and start you connector (both server and client)

    python3 -m aioconnectors create_connector <config_json_path>

If you are testing your connector server and client on the same machine, you can use the configuration generated by print\_config\_templates almost out of the box.  
The only change you should do is set is\_server to False in the client configuration, and use\_ssl to False in both configurations.  
If you want to test messages sending/receiving, you should also set a client\_name in the client configuration. Then you can use the other command line testing facilites mentioned in <a href="#testing">8-</a> : on both server and client you can run "python3 -m aioconnectors test\_receive\_messages <config\_json\_path>" and "python3 -m aioconnectors test\_send\_messages <config\_json\_path>".  

2.2.Programmatically, examples are provided in applications.py and in aioconnectors\_test.py.  
To create and start a connector :

    connector_manager = aioconnectors.ConnectorManager(config_file_path=config_file_path)
    await connector_manager.start_connector()

To stop a connector :

    await connector_manager.stop_connector()

To shutdown a connector :

    await connector_manager.stop_connector(shutdown=True)

You don't have to use a config file (config\_file\_path), you can also directly initialize your ConnectorManager kwargs, as shown in the previous basic examples, and in aioconnectors\_test.py.


<a name="sendreceive"></a>
### 3.send/receive messages with the API

3.1.To configure the Connector API, create a <config\_json\_path> file based on the API template json.
Relevant for both server and client. This connector_api config file is a subset of the connector_manager config file. So if you already have a relevant connector_manager config file on your machine, you can reuse it for connector_api, and you don't need to create a different connector_api config file.

    python3 -m aioconnectors print_config_templates
    connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)

3.2.Or you can directly initialize your ConnectorAPI kwargs  

Then you can send and receive messages by calling the following coroutines in your program, as shown in aioconnectors\_test.py, and in applications.py (test\_receive\_messages and test\_send\_messages).  

3.3.To send messages : 

    await connector_api.send_message(data=None, binary=None, **kwargs)

This returns a status (True or False).  
"data" is your message, "binary" is an optional additional binary message in case you want your "data" to be a json for example.
If your "data" is already a binary, then the "binary" field isn't necessary.  
kwargs contain all the transport instructions for this message, as explained in <a href="#send">5-</a>.  
If you set the await\_response kwarg to True, this returns the response, which is a (transport\_json , data, binary) triplet.  
The received transport\_json field contains all the kwargs sent by the peer.  
You can also send messages synchronously, with :

    connector_api.send_message_sync(data=None, binary=None, **kwargs)

Similarly, use the "publish\_message" and "publish\_message\_sync" methods in the publish/subscribe approach.  
More details in <a href="#send">5-</a>.  

3.4.To register to receive messages of a specific message\_type : 

    await connector_api.start_waiting_for_messages(message_type='', message_received_cb=message_received_cb, reuse_uds_path=False)

-**message\_received\_cb** is an async def coroutine that you must provide, receiving and processing the message quadruplet (logger, transport\_json, data, binary).  
-**transport\_json** is a json with keys related to the "transport layer" of our message protocol : these are the kwargs sent in send_message. They are detailed in <a href="#send">5-</a>. The main arguments are source\_id, destination\_id, request\_id, response\_id, etc.  
Your application can read these transport arguments to obtain information about peer (source\_id, request\_id if provided, etc), and in order to create a proper response (with correct destination\_id, and response\_id for example if needed, etc).  
transport\_json will contain a with\_file key if a file has been received, more details in <a href="#send">5-</a>.  
-**data** is the message data bytes. It is always bytes, so if it was originally sent as a json or a string, you'll have to convert it back by yourself.  
-**binary** is an optional binary message (or None).  
-**reuse_uds_path** is false by default, preventing multiple listeners of same message type. In case it raises an exception even with a single listener, you might want to find and delete an old uds\_path\_receive\_from\_connector file specified in the exception.  
-if you send a message using send\_message(await\_response=True), the response value is the expected response message : so in that case the response message is not received by the start\_waiting\_for\_messages task.


<a name="classes"></a>
### 4.More details about the ConnectorManager and ConnectorAPI arguments.

    logger=None, use_default_logger=True, default_logger_log_level='INFO', default_logger_rotate=True, config_file_path=<path>

config\_file\_path can be the path of a json file like the following, or instead you can load its items as kwargs, as shown in the basic example later on and in aioconnectors\_test.py  
You can use both kwargs and config_file_path : if there are shared items, the ones from config_file_path will override the kwargs.  

Here is an example of config\_file\_path, with ConnectorManager class arguments, used to create a connector

    {
        "certificates_directory_path": "/var/tmp/aioconnectors",
        "client_bind_ip": null,
        "client_name": null,
        "connector_files_dirpath": "/var/tmp/aioconnectors",
        "debug_msg_counts": true,
        "default_logger_dirpath": "/var/tmp/aioconnectors",
        "default_logger_log_level": "INFO",
        "default_logger_rotate": true,
        "disk_persistence_recv": false,
        "disk_persistence_send": false,
        "enable_client_try_reconnect": true,
        "everybody_can_send_messages": true,
        "file_recv_config": {},
        "is_server": true,
        "max_size_file_upload_recv": 8589930194,
        "max_size_file_upload_send": 8589930194,
        "max_size_persistence_path": 1073741824,
        "proxy": {},
        "pubsub_central_broker": false,
        "recv_message_types": [
            "any"
        ],
        "reuse_server_sockaddr": false,
        "reuse_uds_path_commander_server": false,
        "reuse_uds_path_send_to_connector": false,
        "send_message_types": [
            "any"
        ],
        "send_message_types_priorities": {},
        "server_sockaddr": [
            "127.0.0.1",
            10673
        ],
        "silent": true,
        "ssl_allow_all": false,
        "subscribe_message_types": [],
        "uds_path_receive_preserve_socket": true,
        "uds_path_send_preserve_socket": true,
        "use_ssl": true
    }

Here is an example of config\_file\_path, with ConnectorAPI class arguments, used to send/receive messages.  
These are a subset of ConnectorManager arguments : which means you can use the ConnectorManager config file also for ConnectorAPI.


    {
        "client_name": null,
        "connector_files_dirpath": "/var/tmp/aioconnectors",
        "default_logger_dirpath": "/var/tmp/aioconnectors",
        "default_logger_log_level": "INFO",
        "default_logger_rotate": true,
        "is_server": true,
        "pubsub_central_broker": false,
        "receive_from_any_connector_owner": true,
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
        "uds_path_receive_preserve_socket": true,
        "uds_path_send_preserve_socket": true
    }


-**default\_logger\_rotate** (boolean) can also be an integer telling the maximum size of the log file in bytes. There are 5 backups configured.  
-**is\_server** (boolean) is important to differentiate between server and client  
-**server\_sockaddr** can be configured as a tuple when used as a kwarg, or as a list when used in the json, and is mandatory on both server and client sides. You can use an interface name instead of its ip on server side, for example ("eth0", 10673).  
-**client\_name** is used on client side. It is the name that will be associated with this client on server side. Auto generated if not supplied in ConnectorManager. Mandatory in ConnectorAPI.  
-**client_bind_ip** is optional, specifies the interface to bind your client. You can use an interface name or its ip address (string).  
-**use\_ssl** and **ssl\_allow\_all** are boolean, must be identical on server and client. use\_ssl enables encryption as explained previously. When ssl\_allow\_all is disabled, certificates validation is enforced.  
-**certificates\_directory\_path** is where your certificates are located, if use\_ssl is True. This is the <optional\_directory\_path> where you generated your certificates by calling "python3 -m aioconnectors create\_certificates <optional\_directory\_path>".  
-**connector\_files\_dirpath** is important, it is the path where all internal files are stored. The default is /var/tmp/aioconnectors. unix sockets files, default log files, and persistent files are stored there.  
-**pubsub\_central\_broker** : set to True if you need your server to be the broker. Used in the publish/subscribe approach, not necessary in the point to point approach.  
-**send\_message\_types** : the list of message types that can be sent from connector. Default is ["any"] if you don't care to differentiate between message types on your application level.  
-**recv\_message\_types** : the list of message types that can be received by connector. Default is ["any"]. It should include the send\_message\_types using await\_response.  
-**send\_message\_types\_priorities** : None, or a dictionary specifying for each send\_message\_type its priority. The priority is an integer, a smaller integer meaning a higher priority. Usually this is not needed, but with very high throughputs you may want to use it in order to ensure that a specific message type will not get drown by other messages. This might starve the lowest priority messages. Usage example : "send\_message\_types\_priorities": {"type\_fast":0, "type\_slow":1}.  
-In order to be able to receive files, you must define the destination path of files according to their associated dst\_type. This is done in **file\_recv\_config**, as shown in aioconnectors\_test.py. file\_recv\_config = {"target\_directory":"", "owner":"", "override\_existing":False}. **target\_directory** is later formatted using the transport\_json fields : which means you can use a target\_directory value like "/my_destination_files/{message\_type}/{source\_id}". **owner** is optional, it is the owner of the uploaded file. It must be of the form "user:group". **override\_existing** is optional and false by default : when receiving a file with an already existing destination path, it decides whether to override the existing file or not.  
-**hook\_target\_directory** : does not appear in the config file (usable as a kwargs only). A dictionary of the form {dst\_type: custom_function} where custom\_function receives transport\_json as an input and outputs a destination path to be appended to target\_directory. If custom\_function returns None, it has no effect on the target\_directory. This enables better customization of the target\_directory according to transport\_json. An example exists in the chat implementation in applications.py.  
-You can choose the size limit of files you send and receive with **max\_size\_file\_upload**, both on server and on client. Default is 8GB. However best performance is achieved until 1GB. Once you exceed 1GB, the file is divided in 1GB chunks and reassembled after reception, which is time consuming.  
-In order to enable persistence between client and server (supported on both client and server sides), use **disk\_persistence\_send**=True (applies to all message types). disk\_persistence\_send can also be a list of message types for which to apply persistence. There will be 1 persistence file per message type. You can limit the persistence files size with **max\_size\_persistence\_path**.  
-In order to enable persistence between the connector and a message listener (supported on both client and server sides), use **disk\_persistence\_recv**=True (applies to all message types). disk\_persistence\_recv can also be a list of message types for which to apply persistence. There will be 1 persistence file per message type.  
-**subscribe\_message\_types** : In the publish/subscribe approach, specify for your client the message types you want to subscribe to. It is a subset of recv\_message\_types.  
-**uds\_path\_receive\_preserve\_socket** should always be True for better performance, your message\_received\_cb coroutine in start\_waiting\_for\_messages is called for each message without socket disconnection between messages.  
-**uds\_path\_send\_preserve\_socket** should always be True for better performance.  
-**debug_msg_counts** is a boolean, enables to display every 2 minutes a count of messages in the log file, and in stdout if **silent** is disabled.  
-**enable\_client\_try\_reconnect** is a boolean set to True by default. If enabled, it lets the client try to reconnect automatically to the server every 5 seconds in case of failure.  
-**reuse\_server\_sockaddr**, **reuse\_uds\_path\_send\_to\_connector**, **reuse\_uds\_path\_commander\_server** : booleans false by default, that prevent duplicate processes you might create by mistake from using the same sockets. In case your OS is not freeing a closed socket, you still can set the relevant boolean to true.  
-**everybody\_can\_send\_messages** if True lets anyone send messages through the connector, otherwise the sender must have write permission to the connector. Setting to True requires the connector to run as root.  
-**receive\_from\_any\_connector\_owner** if True lets the api receive messages from a connector being run by any user, otherwise the connector user must have write permission to the api. True by default (requires the api to run as root to be effective).  
-**proxy** an optional dictionary like {"enabled":true, "address":"<proxy_url>", "port":<proxy_port>, "authorization":"", "ssl\_server":false}. Relevant only on client side. Lets the client connect to the server through an http(s) proxy with the connect method, if the **enabled** field is true. The authorization field can have a value like {"username":"<username>", "password":"<password>"}. Regardless of the aioconnectors inner encryption, you can set the "ssl\_server" flag in case your proxy listens on ssl : this feature is currently not tested because such proxy setup is rare.  
-**hook\_server\_auth\_client** : does not appear in the config file (usable as a kwargs only). Only for server. Can be an async def coroutine receiving a client peername and returning a boolean, to let the server accept or block the client connection. An example exists in the chat implementation in applications.py.  


<a name="send"></a>
### 5.More details about the send\_message arguments

    send_message(message_type=None, destination_id=None, request_id=None, response_id=None,
    data=None, data_is_json=True, binary=None, await_response=False, with_file=None,
    wait_for_ack=False, await_response_timeout=None) 
    with_file can be like : {'src_path':'','dst_type':'', 'dst_name':'', 'delete':False, 'owner':''}

send_message is an async coroutine.  
These arguments must be filled on the application layer by the user  
-**message\_type** is mandatory, it enables to have different listeners that receive different message types. You can use "any" as a default.  
-**destination\_id** is mandatory for server : it is the remote client id. Not needed by client.  
-**data\_is\_json** is True by default since it assumes "data" is a json, and it dumps it automatically. Set it to False if "data" is not a json.  
-**data** is the payload of your message. By default it expects a json, but it can be a string, and even bytes. However, using together the "data" argument for a json or a string, and the "binary" argument for binary payload, is a nice way to accompany a binary payload with some textual information. Contrary to "data", **binary** must be bytes, and cannot be a string. A message size should not exceed 1GB.  
-**with\_file** lets you embed a file, with {'src\_path':'','dst\_type':'', 'dst\_name':'', 'delete':False, 'owner':''}. **src\_path** is the source path of the file to be sent, **dst\_type** is the type of the file, which enables the remote peer to evaluate the destination path thanks to its ConnectorManager attribute "file\_recv\_config" dictionary. **dst\_name** is the name the file will be stored under. **delete** is a boolean telling if to delete the source file after it has been sent. **owner** is the optional user:group of your uploaded file : if used, it overrides the "owner" value optionally set on server side in file\_recv\_config. If an error occurs while opening the file to send, the file will not be sent but with\_file will still be present in transport\_json received by peer, and will contain an additional key **file\_error** telling the error to the peer application.  
-**request\_id** and **response\_id** are optional (integer or string) : they are helpful to keep track of asynchronous messages on the application layer. At the application level, the remote peer should answer with response\_id equal to the request\_id of the request. The request sender can then associate the received response with the request sent.  
-**await\_response** is False by default, set it to True if your coroutine calling send\_message expects a response value.  
In such a case, the remote peer has to answer with response\_id equal to the request\_id of the request. (This is shown in aioconnectors\_test.py).  
-**wait\_for\_ack** is not recommended for high throughputs, since it slows down dramatically. Basic testing showed a rate of ten messages per second, instead of a few thousands messages per second in the point to point approach (and a few hundreds per second in the slower publish/subscribe approach).  
Not a benchmark, but some point-to-point trials showed that up until 4000 messages (with data of 100 bytes) per second could be received by a server without delay, and from that point the receive queue started to be non empty. This test gave the same result with 100 clients sending each 40 events per second, and with 1 client sending 4000 events per second.  
-**await_response_timeout** is None by default. If set to a number, and if await\_response is true, the method waits up to this timeout for the peer response, and if timeout is exceeded it returns False.  

The **send\_message\_await\_response** method is the same as send_message, but automatically sets await_response to True.  
The **send\_message\_sync** method is almost the same as send_message, but called synchronously (not an async coroutine). It can also receive a "loop" as a kwarg. If a loop is running in the background, it schedules and returns a task. Otherwise it returns the peer response if called with await\_response.  
The **publish\_message** and **publish\_message\_sync** methods are the same as the send_message ones, but used by a client in the publish/subscribe approach.  


<a name="management"></a>
### 6.Management programmatic tools

The class ConnectorManager has several methods to manage your connector. These methods are explained in <a href="#cli">7-</a>.  
-**start\_connector**, **stop\_connector**, **restart\_connector**  
-**delete\_client\_certificate**, **disconnect\_client**  
-**show\_connected\_peers**  
-**delete\_previous\_persistence\_remains**  
-**ignore\_peer\_traffic\_show**, **ignore\_peer\_traffic\_enable**, **ignore\_peer\_traffic\_enable\_unique**, **ignore\_peer\_traffic\_disable**  
-**show\_log\_level**, **set\_log\_level**  
-**show\_subscribe\_message\_types**, **set\_subscribe\_message\_types**  
The same methods can be executed remotely, with the ConnectorRemoteTool class. This class is instantiated exactly like ConnectorAPI, with the same arguments (except for receive_from_any_connector_owner)  

    connector_remote_tool = aioconnectors.ConnectorRemoteTool(config_file_path=config_file_path)

An example of ConnectorRemoteTool is available in applications.py in the cli implementation.


<a name="cli"></a>
### 7.Other management command line tools

    python3 -m aioconnectors cli

to run several interesting commands like :   
-start/stop/restart your connectors.  
-show currently connected peers.  
-delete\_client\_certificate enables your server to delete a specific client certificate. delete\_client\_certificate enables your client to delete its own certificate and fallback using the default one. In order to delete a certificate of a currently connected client, first delete the certificate on server side, which will disconnect the client instantaneously, and then delete the certificate on client side : the client will then reconnect automatically and obtain a new certificate.  
-disconnect_client enables your server to disconnect a specific client.  
-peek\_queues to show the internal queues sizes.  
-ignore\_peer\_traffic can be a boolean, or a peer name. When enabled, the connector drops all new messages received from peers, or from the specified peer. It also drops new messages to be sent to all peers, or to the specified peer. This mode can be useful to let the queues evacuate their accumulated messages.  
-show\_log\_level to show the current log level.  
-set\_log\_level to set the log level on the fly.  
-show\_subscribe\_message\_types to show the subscribed message types of a client.  
-set\_subscribe\_message\_types to set the list of all subscribed message types of a client.  


<a name="testing"></a>
### 8.Testing command line tools

-To let your connector send pings to a remote connector, and print its replies. 

    python3 -m aioconnectors ping <config_json_path>

-To simulate a simple application waiting for messages, and print all received messages. Your application should not wait for incoming messages when using this testing tool.

    python3 -m aioconnectors test_receive_messages <config_json_path>

-To simulate a simple application sending dummy messages.

    python3 -m aioconnectors test_send_messages <config_json_path>


<a name="chat"></a>
### 9.Funny embedded chat

A simple chat using aioconnectors is embedded. It allows you to exchange messages, files and directories easily between 2 Linux or Mac stations.  
It is encrypted, and supports authentication by prompting to accept connections.  
It is not a multi user chat, but more of a tool to easily transfer stuff between your computers.

-On the 1st station (server side), type : 

    python3 -m aioconnectors chat

-Then on the 2nd station (client side), type :

    python3 -m aioconnectors chat --target <server_ip> 

You can execute local shell commands by preceding them with a \"\!\".  
You can also upload files during a chat, by typing \"\!upload \<file or dir path\>\".  
Files are uploaded to your current working directory. A directory is transferred as a zip file.  
You can simply unzip a zip file by using \"\!dezip \<file name\>\".  

-On client side, you can also directly upload a file or directory to the server without opening a chat :

     python3 -m aioconnectors chat --target <server_ip> --upload <file or dir path>

-On server side, you can accept client connections without prompting by specifying --accept :

    python3 -m aioconnectors chat --accept

-More info :

    python3 -m aioconnectors chat --help

-If you need your server to listen on a specific interface :

    python3 -m aioconnectors chat --bind_server_ip <server_ip>

<server\_ip> can be an ip address, or an interface name  

-If you don't want your server to use the default port (10673), use --port on both peers : 

    python3 -m aioconnectors chat --port <port> [--target <server_ip>]


<a name="containers"></a>
## Containers

Connector client and server, as well as connector api, can run in a Docker container, you just need to pip install aioconnectors in a Python image (or any image having Python >= 3.6 and openssl).  
A connector and its connector api must run on the same host, or in the same Kubernetes pod.  
A connector and its connector api can run in the same container, or in different containers. In case you choose to run them in different containers, you must configure their connector_files_dirpath path as a shared volume, in order to let them share their UDS sockets.


<a name="windows"></a>
## Windows ?

To port aioconnectors to Windows, these steps should be taken, and probably more :  
-Replace usage of unix sockets by maybe : local sockets, or named pipes, or uds sockets if and when they are supported.  
Since the implementation relies on unix sockets paths, a possible approach would be to preserve these paths, and manage a mapping between the paths and their corresponding local listening ports.  
-Port the usage of openssl in ssl_helper.py  
-Convert paths  
-Ignore the file uploaded ownership feature  
-Convert the interface to ipaddress function using ip (used for sockaddr and client\_bind\_ip)



