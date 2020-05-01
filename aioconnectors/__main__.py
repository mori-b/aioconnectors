import sys
import os
import logging
import subprocess
import asyncio
import json
import argparse
import signal
import random
import shutil
import zipfile

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
- create_connector <config file path>\n- test_receive_messages <config file path>\n- test_send_messages <config file path>\n- ping <config file path>\n\
- chat [--target <server_ip>] [--upload <path>] [--help]'


    
    
if len(sys.argv) > 1:
    if sys.argv[1] == 'create_certificates':
        
        logger.info('Starting create_certificates')
        if len(sys.argv) == 3:
            if sys.argv[2] == '--help':
                print('create_certificates without argument will create client and server certificates directories under cwd.\nYou can specify a target directory as an optional argument.')
                sys.exit(0)
            certificates_directory_path = full_path(sys.argv[2])
        else:
            certificates_directory_path = None
        res = aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=certificates_directory_path)
        if res is False:
            sys.exit(1)
        
    elif sys.argv[1] == 'print_config_templates':
        Connector = aioconnectors.connectors_core.Connector
    
        manager_config_template = dict(default_logger_log_level='INFO', default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH,
                        is_server=True, server_sockaddr=Connector.SERVER_ADDR, use_ssl=Connector.USE_SSL, ssl_allow_all=False,
                        certificates_directory_path=None, client_name=None, client_bind_ip=None, send_message_types=Connector.DEFAULT_MESSAGE_TYPES, recv_message_types=Connector.DEFAULT_MESSAGE_TYPES, 
                        disk_persistence_send=Connector.DISK_PERSISTENCE_SEND, disk_persistence_recv=Connector.DISK_PERSISTENCE_RECV, max_size_persistence_path=Connector.MAX_SIZE_PERSISTENCE_PATH,
                        file_type2dirpath={}, debug_msg_counts=Connector.DEBUG_MSG_COUNTS, silent=Connector.SILENT, 
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET)
        print('\nMANAGER TEMPLATE, used to create a connector')
        print(json.dumps(manager_config_template, indent=4, sort_keys=True))
                
        api_config_template = dict(default_logger_log_level='INFO', default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        is_server=True, server_sockaddr=Connector.SERVER_ADDR, client_name=None,
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
            name = input(f'\nTo check your server, please type your server <ip> <port> (default port is {Connector.SERVER_ADDR[1]}).\nTo check your client, please type your client name.\nType "q" to quit.\n')
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
            task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))            
            loop.run_until_complete(task_stop)
            del task_stop
            del task_manager
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

    elif sys.argv[1] == 'chat':
        #usage
        #python3 -m aioconnectors chat
        #python3 -m aioconnectors chat --target 127.0.0.1 [--upload <path>]
        #inside chat, possible to type "!exit" to exit, and "!upload <path>" to upload
        print('\nWelcome to aioconnectors chat !')
        print('Usage :\n- Type messages, or !exit to exit, or any shell command preceded by a ! to execute locally\n- !upload <file or dir path> to upload to peer\'s /tmp/aioconnectors\n- !import <downloaded file name> to import a received file from /tmp/aioconnectors to cwd\n- !zimport <downloaded file name> to import and unzip it\n')
        logger = aioconnectors.connectors_core.get_logger(logger_name='chat', first_run=True)
        custom_prompt = 'aioconnectors>> '
        parser = argparse.ArgumentParser()
        parser.add_argument('chat')
        parser.add_argument('--target', nargs='?', default=None, help="server ip, mandatory for client")
        parser.add_argument('--port', nargs='?', default=None, help="server port, optional for server and client")
        parser.add_argument('--bind_server_ip', nargs='?', default=None, help="bind to ip, optional for server")        
        parser.add_argument('--upload', nargs='?', default=False, help="path of directory or file to upload")
        args = parser.parse_args()
        chat_client_name = 'chat_client'
        CONNECTOR_FILES_DIRPATH = '/tmp/aioconnectors'
        delete_connector_dirpath_later = not os.path.exists(CONNECTOR_FILES_DIRPATH)

        loop = asyncio.get_event_loop()
        
        if not args.target:    #means server
            server_sockaddr = (args.bind_server_ip or '0.0.0.0', args.port or aioconnectors.connectors_core.Connector.SERVER_ADDR[1])
            connector_files_dirpath = CONNECTOR_FILES_DIRPATH
            aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=connector_files_dirpath)            
            connector_manager = aioconnectors.ConnectorManager(is_server=True, server_sockaddr=server_sockaddr, use_ssl=True, ssl_allow_all=True,
                                                               connector_files_dirpath=connector_files_dirpath, certificates_directory_path=connector_files_dirpath,
                                                               send_message_types=['any'], recv_message_types=['any'], file_type2dirpath={'any':connector_files_dirpath})
                        
            connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=server_sockaddr, connector_files_dirpath=connector_files_dirpath,
                                                               send_message_types=['any'], recv_message_types=['any'], default_logger_log_level='INFO')
            destination_id = chat_client_name
        else:
            server_sockaddr = (args.target, args.port or aioconnectors.connectors_core.Connector.SERVER_ADDR[1])
            connector_files_dirpath = CONNECTOR_FILES_DIRPATH
            aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=connector_files_dirpath)            
            connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr, use_ssl=True, ssl_allow_all=True,
                                                               connector_files_dirpath=connector_files_dirpath, certificates_directory_path=connector_files_dirpath,
                                                               send_message_types=['any'], recv_message_types=['any'], file_type2dirpath={'any':connector_files_dirpath}, client_name=chat_client_name)

            connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr, connector_files_dirpath=connector_files_dirpath, client_name=chat_client_name,
                                                               send_message_types=['any'], recv_message_types=['any'], default_logger_log_level='INFO')
            destination_id = None
            
            
        task_manager = loop.create_task(connector_manager.start_connector())
        task_recv = task_console = task_send_file = None
        
        async def message_received_cb(transport_json , data, binary):
            if transport_json.get('await_response'):
                loop.create_task(connector_api.send_message(data=data, data_is_json=False, message_type='any', response_id=transport_json['request_id'],
                                                            destination_id=transport_json['source_id']))
            if data:
                print(data.decode())
                print(custom_prompt,end='', flush=True)                

        if not args.upload:        
            task_recv = loop.create_task(connector_api.start_waiting_for_messages(message_type='any', message_received_cb=message_received_cb))
            
        async def send_file(data, destination_id, with_file, delete_after_upload):
            await connector_api.send_message(data=data, data_is_json=False, destination_id=destination_id, await_response=True, request_id=random.randint(1,1000),
                                                        message_type='any', with_file=with_file)
            if delete_after_upload:
                os.remove(delete_after_upload)
            
        class InputProtocolFactory(asyncio.Protocol):
            def connection_made(self, *args, **kwargs):
                super().connection_made(*args, **kwargs)
                print(custom_prompt,end='', flush=True)
                
            def data_received(self, data):
                data = data.decode().strip()
                if data == '!exit':
                    os.kill(os.getpid(), signal.SIGINT)
                    return
                
                if data.startswith('!upload '):
                    try:
                        with_file = None
                        delete_after_upload = False                    
                        upload_path = data[len('!upload '):]
                        
                        if not os.path.exists(upload_path):
                            raise Exception(upload_path + ' does not exist')
                        if os.path.isdir(upload_path):
                            upload_path_zip = f'{upload_path}.zip'
                            if not os.path.exists(upload_path_zip):
                                shutil.make_archive(upload_path, 'zip', upload_path)
                                delete_after_upload = upload_path_zip   
                            upload_path = upload_path_zip
                            #if zip already exists, don't override it, just send it (even if it may not be the correct zip)
                        
                        data = f'Receiving {os.path.join(CONNECTOR_FILES_DIRPATH,upload_path)}'
                        with_file={'src_path':upload_path,'dst_type':'any', 'dst_name':os.path.basename(upload_path), 'delete':False}                   
                        loop.create_task(send_file(data, destination_id, with_file, delete_after_upload))
                    except Exception as exc:
                        res = str(exc)
                        print(custom_prompt,end='', flush=True)
                        print(res)                        
                    print(custom_prompt,end='', flush=True)
                    return
                
                if data.startswith('!import ') or data.startswith('!zimport '):
                    try:
                        target = data.split('import ')[1]
                        #copy target to cwd
                        shutil.copy(os.path.join(CONNECTOR_FILES_DIRPATH, target), target)
                        if data.startswith('!zimport '):
                            target_dir = target.split('.zip')[0]
                            with zipfile.ZipFile(target) as zf:
                                zf.extractall(path=target_dir)
                    except Exception as exc:
                        res = str(exc)
                        print(custom_prompt,end='', flush=True)
                        print(res)
                    print(custom_prompt,end='', flush=True)
                    return                        
                        
                elif data.startswith('!'):
                    data_shell = data[1:]
                    if data_shell:
                        try:
                            res = subprocess.check_output(data_shell, stderr=subprocess.PIPE, shell=True)
                            res = res.decode().strip()
                        except subprocess.CalledProcessError as exc:
                            res = str(exc)
                        print(custom_prompt,end='', flush=True)
                        print(res)
                        print(custom_prompt,end='', flush=True)
                        return

                loop.create_task(connector_api.send_message(data=data, data_is_json=False, destination_id=destination_id, message_type='any'))                   
                print(custom_prompt,end='', flush=True)
            
        async def connect_pipe(loop):
            #print('>> ',end='', flush=True)
            transport, protocol = await loop.connect_read_pipe(InputProtocolFactory, sys.stdin)        
        
        async def upload_file(args, destination_id):
            await asyncio.sleep(3)    #wait for connection      
            upload_path = args.upload
            delete_after_upload = False
            if os.path.isdir(upload_path):
                upload_path_zip = f'{upload_path}.zip'
                if not os.path.exists(upload_path_zip):
                    shutil.make_archive(upload_path, 'zip', upload_path)
                    delete_after_upload = upload_path_zip    
                upload_path = upload_path_zip
                #if zip already exists, don't override it, just send it (even if it may not be the correct zip)
                
            with_file={'src_path':upload_path,'dst_type':'any', 'dst_name':os.path.basename(upload_path), 'delete':False}    
            await send_file('', destination_id, with_file, delete_after_upload)
                
        if not args.upload:
            task_console = loop.create_task(connect_pipe(loop))
        else:
            task_send_file = loop.create_task(upload_file(args, destination_id))
            task_send_file.add_done_callback(lambda inp:os.kill(os.getpid(), signal.SIGINT))

        try:
            loop.run_forever()
        except:    
            print('Connector stopped !')

        task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))            
        loop.run_until_complete(task_stop)
        if task_console:
            del task_console
        if task_recv:
            connector_api.stop_waiting_for_messages(message_type='any')
            del task_recv
        del task_stop
        del task_manager
        del connector_manager
        if delete_connector_dirpath_later and os.path.exists(connector_files_dirpath):
            shutil.rmtree(connector_files_dirpath)
        
                        
    elif sys.argv[1] == '--help':
        print(HELP)
    else:
        print('Unknown command : '+str(sys.argv[1]))
else:
    print(HELP)
