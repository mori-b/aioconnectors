import sys
import os
import logging
import subprocess
import shutil
import asyncio
import json

import aioconnectors


logger = logging.getLogger('aioconnectors_main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

def full_path(the_path):
    return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))

def display_json(the_json, connector=None):
    if connector:
        print('\n', 'Connector : '+str(connector))
    print('\n', json.dumps(the_json, indent=4, sort_keys=True),'')
    
def clearscreen():
    subprocess.call('clear', shell=True)        
    
HELP = '\naioconnectors supported commands :\n\n- print_config_templates\n- create_certificates [optional dirpath]\n\
- cli (start, stop, restart, show_connected_peers, ignore_peer_traffic, peek_queues, delete_client_certificate)\n\
- create_connector <config file path>\n- test_receive_messages <config file path>\n- test_send_messages <config file path>\n- ping <config file path>\n'

if len(sys.argv) > 1:
    if sys.argv[1] == 'create_certificates':
        '''
        
        1) Create Server certificate
        openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout server.key -out server.pem -config csr_details.conf
        put in SERVER_PEM_PATH, SERVER_KEY_PATH, CLIENT_SERVER_CRT_PATH (pem renamed to crt)
        
        2) Create client default certificate
        openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout default.key -out default.pem -config csr_details.conf
        ########openssl req -new -newkey rsa -nodes -x509 -days 3650 -subj '/O=9d2f849c877b4e50b6fccb54d6cd1818' -keyout default.key -out default.pem -config csr_details.conf  #'/O=default/CN=default.cn.com'
        put in CLIENT_PEM_PATH, CLIENT_KEY_PATH, SERVER_CERTS_PATH (only pem)
        
        3) Calculate hash of client default certificate
        openssl x509 -hash -noout -in default.pem    #add '.0' as an extension
        
        4) Create symlink of client default certificate in server directory
        ln -s ../default.pem <hash.0>
        
        '''        
        
        logger.info('Starting create_certificates')
        if len(sys.argv) == 3:
            if sys.argv[2] == '--help':
                print('create_certificates without argument will create client and server certificates directories under cwd.\nYou can specify a target directory as an optional argument.')
                sys.exit(0)
            certificates_directory_path = full_path(sys.argv[2])
        else:
            certificates_directory_path = None
        ssl_helper = aioconnectors.ssl_helper.SSL_helper(logger, is_server=False, certificates_directory_path=certificates_directory_path)

        certificates_path = os.path.join(ssl_helper.BASE_PATH, 'certificates')
        logger.info('Certificates will be created under directory : '+certificates_path)
        if os.path.exists(certificates_path) and os.listdir(certificates_path):
            logger.error(certificates_path+' should be empty before starting this process')
            sys.exit(1)
            
        certificates_path_server = os.path.join(certificates_path, 'server')
        certificates_path_server_client = os.path.join(certificates_path_server, 'client-certs')       
        certificates_path_server_client_sym = os.path.join(certificates_path_server_client, 'symlinks')               
        certificates_path_server_server = os.path.join(certificates_path_server, 'server-cert')                
        certificates_path_client = os.path.join(certificates_path, 'client')        
        certificates_path_client_client = os.path.join(certificates_path_client, 'client-certs')        
        certificates_path_client_server = os.path.join(certificates_path_client, 'server-cert')                
        
        if not os.path.exists(certificates_path_server_server):
            os.makedirs(certificates_path_server_server)
        if not os.path.exists(certificates_path_server_client_sym):
            os.makedirs(certificates_path_server_client_sym)     
        if not os.path.exists(certificates_path_client_server):
            os.makedirs(certificates_path_client_server)          
        if not os.path.exists(certificates_path_client_client):
            os.makedirs(certificates_path_client_client)                   
            
        SERVER_PEM_PATH = os.path.join(certificates_path_server_server, 'server.pem')
        SERVER_KEY_PATH = os.path.join(certificates_path_server_server, 'server.key')
        CLIENT_SERVER_CRT_PATH = os.path.join(certificates_path_client_server, 'server.crt')
        
        CLIENT_PEM_PATH = os.path.join(certificates_path_client_client, '{}.pem')
        CLIENT_KEY_PATH = os.path.join(certificates_path_client_client, '{}.key')
        SERVER_CERTS_PATH = os.path.join(certificates_path_server_client, '{}.pem')
        
        csr_details_conf = os.path.join(certificates_path_server, 'csr_details.conf')
        
        with open(csr_details_conf, 'w') as fd:
            fd.write(
                '''
                [req]
                prompt = no
                default_bits = 2048
                default_md = sha256
                distinguished_name = dn
                
                [ dn ]
                C=US
                O=Company)
                ''')
        
#        1) Create Server certificate            
        cmd = f'openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {SERVER_KEY_PATH} -out {SERVER_PEM_PATH} -config {csr_details_conf}'
        stdout = subprocess.check_output(cmd, shell=True)
        shutil.copy(SERVER_PEM_PATH, CLIENT_SERVER_CRT_PATH)
 
#        2) Create client default certificate
        client_default_key = CLIENT_KEY_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)
        client_default_pem = CLIENT_PEM_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)       
        cmd = f'openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {client_default_key} -out {client_default_pem} -config {csr_details_conf}'
        stdout = subprocess.check_output(cmd, shell=True)        
        shutil.copy(client_default_pem, SERVER_CERTS_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME))

#        3) Calculate hash of client default certificate
        cmd = f'openssl x509 -hash -noout -in {client_default_pem}'
        stdout = subprocess.check_output(cmd, shell=True)
        the_hash_name = stdout.decode().strip()+'.0'
        
#        4) Create symlink of client default certificate in server directory
        dst = os.path.join(certificates_path_server_client_sym, the_hash_name)
        if os.path.exists(dst):
            os.remove(dst)
        
        cmd = f'ln -s ../{ssl_helper.CLIENT_DEFAULT_CERT_NAME}.pem '+dst
        stdout = subprocess.check_output(cmd, shell=True)
        logger.info('Finished create_certificates')
        
    elif sys.argv[1] == 'print_config_templates':
        Connector = aioconnectors.connectors_core.Connector
    
        manager_config_template = dict(default_logger_log_level='INFO', default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH,
                        is_server=False, server_sockaddr=Connector.SERVER_ADDR, use_ssl=Connector.USE_SSL, 
                        certificates_directory_path=None, client_name=None, send_message_types=Connector.DEFAULT_MESSAGE_TYPES, recv_message_types=Connector.DEFAULT_MESSAGE_TYPES, 
                        disk_persistence=Connector.DISK_PERSISTENCE, max_size_persistence_path=Connector.MAX_SIZE_PERSISTENCE_PATH,
                        file_type2dirpath={}, debug_msg_counts=Connector.DEBUG_MSG_COUNTS, silent=Connector.SILENT, 
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET)
        print('\nMANAGER TEMPLATE, used to create a connector')
        print(json.dumps(manager_config_template, indent=4, sort_keys=True))
                
        api_config_template = dict(default_logger_log_level='INFO', default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        is_server=False, server_sockaddr=Connector.SERVER_ADDR, client_name=None,
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                        send_message_types=Connector.DEFAULT_MESSAGE_TYPES, recv_message_types=Connector.DEFAULT_MESSAGE_TYPES)                 
        print('\nAPI TEMPLATE, used to send/receive messages')
        print(json.dumps(api_config_template, indent=4, sort_keys=True))

    elif sys.argv[1] == 'cli':
        print('\nWelcome to aioconnectors CLI')
        Connector = aioconnectors.connectors_core.Connector   
        
        the_path = input('\nPlease type your connector_files_dirpath, or Enter if it is '+Connector.CONNECTOR_FILES_DIRPATH+'\n')
        if the_path:
            the_path = os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))
        else:
            the_path = Connector.CONNECTOR_FILES_DIRPATH
        
        while True:
            name = input('\nTo check your server, please type your server <ip> <port>.\nTo check your client, please type your client name.\nType "q" to quit.\n')
            if name == 'q':
                sys.exit(0)
            names = name.split(maxsplit=1) #assume that client name has no spaces
            server_sockaddr = client_name = None
            if len(names) == 2:
                server_sockaddr = (names[0], int(names[1]))
            else:
                client_name = name
            
            loop = asyncio.get_event_loop()
            is_server = (server_sockaddr is not None)
            connector_remote_tool = aioconnectors.ConnectorRemoteTool(use_default_logger=False, is_server=is_server, server_sockaddr=server_sockaddr, client_name=client_name, connector_files_dirpath=the_path)        
            list_cmds = ['start', 'stop gracefully', 'stop hard', 'restart', 'show_connected_peers', 'ignore_peer_traffic', 'peek_queues', 'delete_client_certificate']
            dict_cmds = {str(index):cmd for index,cmd in enumerate(list_cmds)}
            display_json(dict_cmds, connector=server_sockaddr or client_name)        
            res = input('\nPlease type the command number you would like to run, or q to quit\n')
            
            while True:
                clearscreen()
                if res == 'q':
                    break
                if res not in dict_cmds:
                    print('Invalid number : '+str(res))
                else:
                    the_cmd = dict_cmds[res]
                    if the_cmd == 'start':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='start', kwargs={'connector_socket_only':False}))
                        loop.run_until_complete(task)
                        print(task.result().decode())                        
                    elif the_cmd == 'stop gracefully':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='stop', kwargs={'connector_socket_only':False, 'client_wait_for_reconnect':False, 'hard':False}))
                        loop.run_until_complete(task)
                        print(task.result().decode())
                    elif the_cmd == 'stop hard':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='stop', kwargs={'connector_socket_only':False, 'client_wait_for_reconnect':False, 'hard':True}))
                        loop.run_until_complete(task)
                        print(task.result().decode())                    
                    elif the_cmd == 'restart':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='restart', kwargs={'sleep_between':2, 'connector_socket_only':False, 'hard':False}))
                        loop.run_until_complete(task)
                        print(task.result().decode())
                    elif the_cmd == 'show_connected_peers':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='show_connected_peers__sync', kwargs={}))
                        loop.run_until_complete(task)
                        print(task.result().decode())                        
                    elif the_cmd == 'ignore_peer_traffic':
                        while True:
                            task = loop.create_task(connector_remote_tool.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'show':True}))
                            loop.run_until_complete(task)
                            status = task.result().decode()
                            print('\nignore_peer_traffic current status : ', status)
                            if status == 'False':
                                res = input('\nType "y" to ignore peer traffic, or <peer name> to ignore a unique peer traffic, or Enter to quit\n')
                                if res == 'y':
                                    task = loop.create_task(connector_remote_tool.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'enable':True}))
                                    loop.run_until_complete(task)
                                    continue
                                elif res == '':
                                    break
                                else:
                                    task = loop.create_task(connector_remote_tool.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'unique_peer':res}))
                                    loop.run_until_complete(task)
                                    continue                                    
                            else:
                                res = input('\nType "y" to stop ignoring peer traffic, or Enter to quit\n')
                                if res == 'y':
                                    task = loop.create_task(connector_remote_tool.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'disable':True}))
                                    loop.run_until_complete(task)
                                    continue
                                else:
                                    break                                
                    elif the_cmd == 'peek_queues':
                        task = loop.create_task(connector_remote_tool.send_command(cmd='peek_queues__sync', kwargs={'dump_result':True}))
                        loop.run_until_complete(task)
                        print(json.dumps(json.loads(task.result().decode()), indent=4, sort_keys=True))
                    elif the_cmd == 'delete_client_certificate':
                        if is_server:
                            client_name = input('\nPlease type the client name whose certificate you would like to delete, or q to quit\n')
                            res = input('\nAre you sure you want to delete '+client_name+' \'s certificate ? y/n\n')
                            if res =='y':
                                task = loop.create_task(connector_remote_tool.delete_client_certificate(client_id=client_name, remove_only_symlink=False))                            
                        else:
                            res = input('\nAre you sure you want to delete '+client_name+' \'s certificate ? y/n\n')
                            if res =='y':                            
                                task = loop.create_task(connector_remote_tool.delete_client_certificate())
                        loop.run_until_complete(task)
                        print(task.result().decode())
                        
                        
                display_json(dict_cmds, connector=server_sockaddr or client_name)                    
                res = input('\nPlease type the command number you would like to run, or q to quit\n')                    

    elif sys.argv[1] == 'create_connector':
        if len(sys.argv) != 3:
            print('Usage : create_connector <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : create_connector <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        logger.info('Creating connector with config file '+config_file_path)
        connector_manager = aioconnectors.ConnectorManager(config_file_path=config_file_path)
        loop = asyncio.get_event_loop()
        #task = loop.create_task(connector_manager.delete_previous_persistence_remains())
        #loop.run_until_complete(task)
        task_manager = loop.create_task(connector_manager.start_connector())
        try:
            loop.run_forever()
        except:    
            task_stop = loop.create_task(connector_manager.connector.stop(connector_socket_only=False, client_wait_for_reconnect=False, hard=False))            
            loop.run_until_complete(task_stop)
            task_manager.cancel()
            del task_manager
            connector_manager.connector.shutdown_sync()            
            del connector_manager
            print('Connector stopped !')        

    elif sys.argv[1] == 'test_receive_messages':
        if len(sys.argv) != 3:
            print('Usage : test_receive_messages <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : test_receive_messages <config file path>')
            sys.exit(0)
        print('Warning : No other application should be receiving events from this connector')
        config_file_path=sys.argv[2]            
        logger.info('Creating connector api with config file '+config_file_path)
        connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)
        loop = asyncio.get_event_loop()
        tasks_waiting_for_messages = []
        
        async def message_received_cb(transport_json , data, binary):
            print(f'RECEIVED MESSAGE {transport_json}')
            if data:
                print(f'\tWith data {data.decode()}')
            if binary:
                print(f'\tWith binary {binary}')
        
        for message_type in connector_api.recv_message_types:
            task_api = loop.create_task(connector_api.start_waiting_for_messages(message_type=message_type, message_received_cb=message_received_cb))
            tasks_waiting_for_messages.append(task_api)
        try:
            loop.run_forever()
        except:
            for message_type in connector_api.recv_message_types:
                connector_api.stop_waiting_for_messages(message_type=message_type)            
            #for task_api in tasks_waiting_for_messages:
            #    task_api.cancel()
            print('test_receive_messages stopped !')

    elif sys.argv[1] == 'test_send_messages':
        if len(sys.argv) != 3:
            print('Usage : test_send_messages <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : test_send_messages <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        logger.info('Creating connector api with config file '+config_file_path)
        connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)
        destination_id = None
        if connector_api.is_server:
            destination_id = input('\nPlease type the name of your remote client\n')
            
        loop = asyncio.get_event_loop()
        
        async def send_messages(destination_id):
            index = 0
            while True:
                index += 1
                for message_type in connector_api.send_message_types:
                    print(f'SENDING MESSAGE to peer {destination_id or connector_api.server_sockaddr} of type {message_type} and index {index}')
                    response = await connector_api.send_message(data=f'"TEST_MESSAGE {str(index)*5}"', data_is_json=False, destination_id=destination_id,
                                                            message_type=message_type, await_response=False, request_id=index)
                                                            #response_id=None, binary=b'\x01\x02\x03\x04\x05', with_file=None, wait_for_ack=False)        
                await asyncio.sleep(2)
                        
        task_send = loop.create_task(send_messages(destination_id))
        try:
            loop.run_forever()
        except:
            task_send.cancel()
            print('test_send_messages stopped !')
            
    elif sys.argv[1] == 'ping':
        if len(sys.argv) != 3:
            print('Usage : ping <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : ping <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        logger.info('Creating connector api with config file '+config_file_path)
        connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)
        destination_id = None
        if connector_api.is_server:
            destination_id = input('\nPlease type the name of your remote client\n')
            
        loop = asyncio.get_event_loop()
        
        async def send_messages(destination_id):
            index = 0
            while True:
                index += 1
                print(f'\nSENDING PING to peer {destination_id or connector_api.server_sockaddr} with index {index}')
                response = await connector_api.send_message_await_response(data=f'PING {str(index)*5}', data_is_json=False, destination_id=destination_id,
                                                        message_type='_ping', request_id=index)
                                                        #response_id=None, binary=b'\x01\x02\x03\x04\x05', with_file=None, wait_for_ack=False)
                if response:
                    try:
                        transport_json, data, binary = response
                    except Exception as exc:
                        print(exc)
                        return
                    print(f'RECEIVING REPLY from peer {destination_id or connector_api.server_sockaddr} with data {data}')                                                        
                await asyncio.sleep(2)
                        
        task_send = loop.create_task(send_messages(destination_id))
        try:
            loop.run_forever()
        except:
            task_send.cancel()
            print('ping stopped !')
                    
    elif sys.argv[1] == '--help':
        print(HELP)
    else:
        print('Unknown command : '+str(sys.argv[1]))
else:
    print(HELP)