import asyncio
import os
import random
import signal
import shutil
import zipfile
import subprocess
import sys
import json
import re

import aioconnectors

REGEX_IP = re.compile('^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')

def create_connector(config_file_path, logger=None):
    if not logger:
        logger = aioconnectors.get_logger(logger_name='create_connector', first_run=True)    
    logger.info('Creating connector with config file '+config_file_path)
    connector_manager = aioconnectors.ConnectorManager(config_file_path=config_file_path)
    loop = asyncio.get_event_loop()
    #task = loop.create_task(connector_manager.delete_previous_persistence_remains())
    #loop.run_until_complete(task)
    task_manager = loop.create_task(connector_manager.start_connector())
    #run_until_complete now, in order to exit in case of exception
    #for example because of already existing socket
    loop.run_until_complete(task_manager)
    
    try:
        loop.run_forever()
    except:    
        task_stop = loop.create_task(connector_manager.stop_connector(delay=None, hard=False, shutdown=True))            
        loop.run_until_complete(task_stop)
        del task_stop
        del task_manager
        del connector_manager
        print('Connector stopped !')
        
def cli(logger=None):
    def clearscreen():
        subprocess.call('clear', shell=True)        
    def display_dict(the_json, connector=None):
        if connector:
            print('\n', 'Connector : '+str(connector))
        #print('\n', json.dumps(the_json, indent=4, sort_keys=True),'')
        for the_key, value in sorted(the_json.items(), key=lambda x:int(x[0])):
            print(str(the_key)+')    '+str(value))
    
    if not logger:
        logger = aioconnectors.get_logger(logger_name='cli', first_run=True)
    running_with_tab_completion = True
    try:    
        import readline
        readline.set_completer_delims('\t\n= ')
        readline.parse_and_bind('tab:complete')
    except Exception:
        running_with_tab_completion = False
        logger.info('Running without tab completion')
        
    print('\nWelcome to aioconnectors CLI')
    Connector = aioconnectors.core.Connector    
    the_path = input('\nPlease type your connector_files_dirpath, or Enter if it is '
                     f'{Connector.CONNECTOR_FILES_DIRPATH}\n')
    if the_path:
        the_path = os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))
    else:
        the_path = Connector.CONNECTOR_FILES_DIRPATH
        
    while True:
        active_connectors_path = os.path.join(the_path, Connector.DEFAULT_ACTIVE_CONNECTORS_NAME)
        dict_connector_names = {}
        try:         
            if os.path.exists(active_connectors_path):
                with open(active_connectors_path, 'r') as fd:
                    set_active_connectors = json.load(fd)    
                dict_connector_names = {str(index):connector_name for index,connector_name in \
                                            enumerate(sorted(set_active_connectors))}
                display_dict(dict_connector_names)        
                print('\nPlease type the connector number you would like to run, or')
        except Exception as exc:
            print(exc)
        
        name_input = input(f'\nTo check your server, please type your server <ip> <port> '
                     f'(default port is {Connector.SERVER_ADDR[1]}).\nTo check your client, please type your '
                     'client name.\nType "q" to quit.\n')        
        if name_input == 'q':
            sys.exit(0)
        name = dict_connector_names.get(name_input, name_input)
        names = name.split(maxsplit=1) #assume that client name has no spaces
        server_sockaddr = client_name = None
        if len(names) == 2:
            server_sockaddr = (names[0], int(names[1]))
        else:
            client_name = name
        
        loop = asyncio.get_event_loop()
        is_server = (server_sockaddr is not None)
        connector_remote_tool = aioconnectors.ConnectorRemoteTool(use_default_logger=False, is_server=is_server, 
                                                server_sockaddr=server_sockaddr, client_name=client_name, 
                                                connector_files_dirpath=the_path)
        if not os.path.exists(connector_remote_tool.connector.uds_path_commander):
            clearscreen()
            print(f'The connector {name} does not exist')
            if name_input in dict_connector_names:
                #deleting invalid name_input from  dict_connector_names
                set_active_connectors.remove(name)
                with open(active_connectors_path, 'w') as fd:
                    json.dump(set_active_connectors, fd)
            continue
        clearscreen()
        list_cmds = ['start', 'stop gracefully', 'stop hard', 'restart', 'show_connected_peers', 
                     'ignore_peer_traffic', 'peek_queues', 'delete_client_certificate', 'delete_client_token', 'disconnect_client',
                     'show_log_level', 'set_log_level', 'show_subscribe_message_types', 'set_subscribe_message_types',
                     'add_blacklist_client', 'remove_blacklist_client', 'add_whitelist_client', 'remove_whitelist_client']
        dict_cmds = {str(index):cmd for index,cmd in enumerate(list_cmds)}
        display_dict(dict_cmds, connector=server_sockaddr or client_name)        
        res = input('\nPlease type the command number you would like to run, or q to quit\n')

        def show_connected_peers(return_peers=False):
            task = loop.create_task(connector_remote_tool.show_connected_peers())
            loop.run_until_complete(task)
            peers_dict = task.result().decode()
            print(f'\nConnected peers : {peers_dict}')
            if return_peers:
                return json.loads(peers_dict)
            
        def show_attribute(attribute):
            show_attribute            
            task = loop.create_task(connector_remote_tool.show_attribute(attribute))
            loop.run_until_complete(task)
            value = task.result().decode()
            print(f'\n{attribute.capitalize()} : {value}')
            
        while True:
            clearscreen()
            if res == 'q':
                break
            if res not in dict_cmds:
                print('Invalid number : '+str(res))
            else:
                the_cmd = dict_cmds[res]
                
                if the_cmd == 'start':
                    task = loop.create_task(connector_remote_tool.start_connector())
                    loop.run_until_complete(task)
                    print(task.result().decode())           
                    
                elif the_cmd == 'stop gracefully':
                    task = loop.create_task(connector_remote_tool.stop_connector(client_wait_for_reconnect=False, hard=False))
                    loop.run_until_complete(task)
                    print(task.result().decode())
                    
                elif the_cmd == 'stop hard':
                    task = loop.create_task(connector_remote_tool.stop_connector(client_wait_for_reconnect=False, hard=True))
                    loop.run_until_complete(task)
                    print(task.result().decode())          
                    
                elif the_cmd == 'restart':
                    task = loop.create_task(connector_remote_tool.restart_connector(sleep_between=2, hard=False))
                    loop.run_until_complete(task)
                    print(task.result().decode())
                    
                elif the_cmd == 'show_connected_peers':
                    show_connected_peers()             
                    
                elif the_cmd == 'ignore_peer_traffic':
                    while True:
                        task = loop.create_task(connector_remote_tool.ignore_peer_traffic_show())
                        loop.run_until_complete(task)
                        status = task.result().decode()
                        print('\nignore_peer_traffic current status : ', status)
                        if status == 'False':
                            
                            peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                            if running_with_tab_completion:
                                def complete(text,state):
                                    results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                    return results[state]
                                readline.set_completer(complete)                        

                            res = input('\nType "y" to ignore peer traffic, or <peer name> to ignore a unique peer '
                                        'traffic, or q to quit\n')
                            
                            if running_with_tab_completion:
                                readline.set_completer(None) 
                            
                            if res == 'y':
                                task = loop.create_task(connector_remote_tool.ignore_peer_traffic_enable())
                                loop.run_until_complete(task)
                                continue
                            elif res == 'q':
                                break
                            else:
                                task = loop.create_task(connector_remote_tool.ignore_peer_traffic_enable_unique(res))
                                loop.run_until_complete(task)
                                continue                                    
                        else:
                            res = input('\nType "y" to stop ignoring peer traffic, or Enter to quit\n')
                            if res == 'y':
                                task = loop.create_task(connector_remote_tool.ignore_peer_traffic_disable())
                                loop.run_until_complete(task)
                                continue
                            else:
                                break       
                            
                elif the_cmd == 'peek_queues':
                    task = loop.create_task(connector_remote_tool.peek_queues())
                    loop.run_until_complete(task)
                    print(json.dumps(json.loads(task.result().decode()), indent=4, sort_keys=True))
                    
                elif the_cmd == 'delete_client_certificate':
                    if is_server:
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        client_name = input('\nPlease type the client name whose certificate you would '
                                            'like to delete, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                        
                        if client_name != 'q':
                            res = input('\nAre you sure you want to delete '+client_name+' \'s certificate ? y/n\n')
                            if res =='y':
                                task = loop.create_task(connector_remote_tool.delete_client_certificate(client_id=client_name, 
                                                                                                remove_only_symlink=False))
                                loop.run_until_complete(task)
                                print(task.result().decode())                            
                                task = loop.create_task(connector_remote_tool.disconnect_client(client_id=client_name))
                                loop.run_until_complete(task)
                                print(task.result().decode())                            
                    else:
                        res = input('\nAre you sure you want to delete '+client_name+' \'s certificate ? y/n\n')
                        if res =='y':                            
                            task = loop.create_task(connector_remote_tool.delete_client_certificate())
                            loop.run_until_complete(task)
                            print(task.result().decode())

                elif the_cmd == 'delete_client_token':
                    if is_server:
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        client_name = input('\nPlease type the client name whose token you would '
                                            'like to delete, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                        
                        if client_name != 'q':
                            res = input('\nAre you sure you want to delete '+client_name+' \'s token ? y/n\n')
                            if res =='y':
                                task = loop.create_task(connector_remote_tool.delete_client_token(client_id=client_name))
                                loop.run_until_complete(task)
                                print(task.result().decode())                            
                                task = loop.create_task(connector_remote_tool.disconnect_client(client_id=client_name))
                                loop.run_until_complete(task)
                                print(task.result().decode())                            
                    else:
                        res = input('\nAre you sure you want to delete '+client_name+' \'s token ? y/n\n')
                        if res =='y':                            
                            task = loop.create_task(connector_remote_tool.delete_client_token())
                            loop.run_until_complete(task)
                            print(task.result().decode())
                            
                elif the_cmd == 'disconnect_client':
                    if is_server:
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        client_name = input('\nPlease type the client name you would '
                                            'like to disconnect, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                            
                        if client_name != 'q':                        
                            res = input('\nAre you sure you want to disconnect '+client_name+' ? y/n\n')
                            if res =='y':
                                task = loop.create_task(connector_remote_tool.disconnect_client(client_id=client_name))
                                loop.run_until_complete(task)
                                print(task.result().decode())                          
                    else:
                        print('A client cannot use this functionality')         

                elif the_cmd == 'add_blacklist_client':
                    if is_server:
                        show_attribute('blacklisted_clients_id')
                        show_attribute('blacklisted_clients_ip')
                        show_attribute('blacklisted_clients_subnet')                        
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        the_client = input('\nPlease type the client name (or regex) you would like to blacklist (and disconnect),'
                                            ' or the client IP address/subnet you would like to blacklist, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                            
                        if the_client != 'q':                        
                            res = input('\nAre you sure you want to blacklist '+the_client+' ? y/n\n')
                            if res =='y':
                                #As opposed to ip, the id follows SOURCE_ID_REGEX, which excludes dots
                                if REGEX_IP.match(the_client):
                                    task = loop.create_task(connector_remote_tool.add_blacklist_client(client_ip=the_client))
                                else:
                                    task = loop.create_task(connector_remote_tool.add_blacklist_client(client_id=the_client))
                                loop.run_until_complete(task)
                                print(task.result().decode())                          
                    else:
                        print('A client cannot use this functionality')         

                elif the_cmd == 'remove_blacklist_client':
                    if is_server:
                        show_attribute('blacklisted_clients_id')
                        show_attribute('blacklisted_clients_ip')
                        show_attribute('blacklisted_clients_subnet')                                                
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        the_client = input('\nPlease type the client name (or regex) you would like to remove from blacklist,'
                                            ' or the client IP address/subnet you would like to remove from blacklist, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                            
                        if the_client != 'q':                        
                            res = input('\nAre you sure you want to remove from blacklist '+the_client+' ? y/n\n')
                            if res =='y':
                                if REGEX_IP.match(the_client):
                                    task = loop.create_task(connector_remote_tool.remove_blacklist_client(client_ip=the_client))
                                else:
                                    task = loop.create_task(connector_remote_tool.remove_blacklist_client(client_id=the_client))
                                loop.run_until_complete(task)
                                print(task.result().decode())                          
                    else:
                        print('A client cannot use this functionality')         

                elif the_cmd == 'add_whitelist_client':
                    if is_server:
                        show_attribute('whitelisted_clients_id')
                        show_attribute('whitelisted_clients_ip')
                        show_attribute('whitelisted_clients_subnet')                        
                        
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        the_client = input('\nPlease type the client name (or regex) you would like to whitelist,'
                                            ' or the client IP address/subnet you would like to whitelist, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                            
                        if the_client != 'q':                        
                            res = input('\nAre you sure you want to whitelist '+the_client+' ? y/n\n')
                            if res =='y':
                                if REGEX_IP.match(the_client):
                                    task = loop.create_task(connector_remote_tool.add_whitelist_client(client_ip=the_client))
                                else:
                                    task = loop.create_task(connector_remote_tool.add_whitelist_client(client_id=the_client))
                                loop.run_until_complete(task)
                                print(task.result().decode())                          
                    else:
                        print('A client cannot use this functionality')         

                elif the_cmd == 'remove_whitelist_client':
                    if is_server:
                        show_attribute('whitelisted_clients_id')
                        show_attribute('whitelisted_clients_ip')
                        show_attribute('whitelisted_clients_subnet')                        
                        
                        peers_dict = show_connected_peers(return_peers=running_with_tab_completion)
                        if running_with_tab_completion:
                            def complete(text,state):
                                results = [peer for peer in peers_dict if peer.startswith(text)] + [None]
                                return results[state]
                            readline.set_completer(complete)                        
                        
                        the_client = input('\nPlease type the client name (or regex) you would like to remove from whitelist,'
                                            ' or the client IP address/subnet you would like to remove from whitelist, or q to quit\n')
                        if running_with_tab_completion:
                            readline.set_completer(None) 
                            
                        if the_client != 'q':                        
                            res = input('\nAre you sure you want to remove from whitelist '+the_client+' ? y/n\n')
                            if res =='y':
                                if REGEX_IP.match(the_client):
                                    task = loop.create_task(connector_remote_tool.remove_whitelist_client(client_ip=the_client))
                                else:
                                    task = loop.create_task(connector_remote_tool.remove_whitelist_client(client_id=the_client))
                                loop.run_until_complete(task)
                                print(task.result().decode())                          
                    else:
                        print('A client cannot use this functionality')         
                        
                elif the_cmd == 'show_log_level':
                    task = loop.create_task(connector_remote_tool.show_log_level())
                    loop.run_until_complete(task)
                    print(task.result().decode())      
                             
                elif the_cmd == 'set_log_level':
                    list_levels = ['ERROR', 'WARNING', 'INFO', 'DEBUG']
                    dict_levels = {str(index):level for index,level in enumerate(list_levels)}
                    display_dict(dict_levels)        
                    res = input('\nPlease type the log level you would like to set, or q to quit\n')
                    clearscreen()                    
                    if res == 'q':
                        break
                    if res not in dict_levels:
                        print('Invalid number : '+str(res))
                        break
                    new_level = dict_levels[res]
                    task = loop.create_task(connector_remote_tool.set_log_level(new_level))
                    loop.run_until_complete(task)
                    print(task.result().decode())       
                    
                elif the_cmd == 'show_subscribe_message_types':
                    if is_server:
                        print('Only available for clients')
                    else:
                        task = loop.create_task(connector_remote_tool.show_subscribe_message_types())
                        loop.run_until_complete(task)
                        print(task.result().decode())     
                        
                elif the_cmd == 'set_subscribe_message_types':
                    if is_server:
                        print('Only available for clients')
                    else:                    
                        print('Current subscribed message types are :')
                        task = loop.create_task(connector_remote_tool.show_subscribe_message_types())
                        loop.run_until_complete(task)
                        print(task.result().decode())
                        res = input('\nPlease type the list of all message types you would like to subscribe, or q to quit\n')
                        clearscreen()                    
                        if res == 'q':
                            break
                        new_message_types = res.split()                        
                        res2 = input(f'\nAre you sure you want to subscribe to these message types : {new_message_types} ? y/n\n')
                        if res2.lower() != 'y':
                            break      
                        task = loop.create_task(connector_remote_tool.set_subscribe_message_types(*new_message_types))
                        loop.run_until_complete(task)
                        print(task.result().decode())   
                    
            display_dict(dict_cmds, connector=server_sockaddr or client_name)                    
            res = input('\nPlease type the command number you would like to run, or q to quit\n')                    
        
        
def test_receive_messages(config_file_path, logger=None):
    if not logger:
        logger = aioconnectors.get_logger(logger_name='test_receive_messages', first_run=True)    
    print('Warning : No other application should be receiving events from this connector')
    logger.info('Creating connector api with config file '+config_file_path)
    connector_api = aioconnectors.ConnectorAPI(config_file_path=config_file_path)
    loop = asyncio.get_event_loop()
    tasks_waiting_for_messages = []
    
    async def message_received_cb(logger, transport_json , data, binary):
        logger.info(f'RECEIVED MESSAGE {transport_json}')
        print(f'RECEIVED MESSAGE {transport_json}')
        if data:
            print(f'\tWith data {data.decode()}')
        if binary:
            print(f'\tWith binary {binary}')
    
    for message_type in connector_api.recv_message_types:
        task_api = loop.create_task(connector_api.start_waiting_for_messages(message_type=message_type, 
                                                                             message_received_cb=message_received_cb))
        tasks_waiting_for_messages.append(task_api)
    try:
        loop.run_forever()
    except:
        for message_type in connector_api.recv_message_types:
            connector_api.stop_waiting_for_messages(message_type=message_type)            
        #for task_api in tasks_waiting_for_messages:
        #    task_api.cancel()
        print('test_receive_messages stopped !')


def test_send_messages(config_file_path, logger=None):
    if not logger:
        logger = aioconnectors.get_logger(logger_name='test_send_messages', first_run=True)    
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
                print(f'SENDING MESSAGE to peer {destination_id or connector_api.server_sockaddr} of type '
                      f'{message_type} and index {index}')
                response = await connector_api.send_message(data=f'"TEST_MESSAGE {str(index)*5}"', data_is_json=False,
                                                                                  destination_id=destination_id,
                                                        message_type=message_type, await_response=False, request_id=index)
                                                        #response_id=None, binary=b'\x01\x02\x03\x04\x05', with_file=None, wait_for_ack=False)        
            await asyncio.sleep(2)
                    
    task_send = loop.create_task(send_messages(destination_id))
    try:
        loop.run_forever()
    except:
        task_send.cancel()
        print('test_send_messages stopped !')
        
def test_publish_messages(config_file_path, logger=None):
    if not logger:
        logger = aioconnectors.get_logger(logger_name='test_publish_messages', first_run=True)    
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
                if message_type == '_pubsub':
                    continue
                print(f'PUBLISHING MESSAGE to peer {destination_id or connector_api.server_sockaddr} of type '
                      f'{message_type} and index {index}')
                response = await connector_api.publish_message(data=f'"TEST_MESSAGE {str(index)*5}"', data_is_json=False,
                                                                                  destination_id=destination_id,
                                                        message_type=message_type, await_response=False, request_id=index)
                                                        #response_id=None, binary=b'\x01\x02\x03\x04\x05', with_file=None, wait_for_ack=False)        
            await asyncio.sleep(2)
                    
    task_send = loop.create_task(send_messages(destination_id))
    try:
        loop.run_forever()
    except:
        task_send.cancel()
        print('test_publish_messages stopped !')
        
def ping(config_file_path, logger=None):
    #lets a connector ping a remote connector peer
    if not logger:
        logger = aioconnectors.get_logger(logger_name='ping', first_run=True)    
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
            response = await connector_api.send_message_await_response(data=f'PING {str(index)*5}', data_is_json=False, 
                                                destination_id=destination_id, message_type='_ping', request_id=index)
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
    

def chat(args, logger=None):
    #chat supports sending messages and files/directories between 2 connectors
    if not logger:
        logger = aioconnectors.get_logger(logger_name='chat', first_run=True)

    if not args.nowrap and not args.upload:        
        try:    
            import readline
            readline.set_completer_delims('\t\n= ')            
            readline.parse_and_bind('tab:complete')
        except Exception:
            logger.info('Running without tab completion')
        else:    
            proc = subprocess.Popen(
                    [sys.executable, '-m', 'aioconnectors'] + sys.argv[1:] + ['--nowrap'],
                    bufsize=0,
                    stdin=subprocess.PIPE, stdout=None, stderr=None)
            while True:
                try:
                    user_input = input('')                    
                    proc.stdin.write((user_input + os.linesep).encode())
                    if user_input == '!exit':
                        return 
                except KeyboardInterrupt:
                    proc.kill()
                    return
        
    custom_prompt = 'aioconnectors>> '        
    chat_client_name = 'chat_client'
    CONNECTOR_FILES_DIRPATH = aioconnectors.get_tmp_dir()
    if os.path.exists(CONNECTOR_FILES_DIRPATH):
        res = input(f'May I delete the content of {CONNECTOR_FILES_DIRPATH} ? y/n\n')
        if res == 'y':
            shutil.rmtree(CONNECTOR_FILES_DIRPATH)
    
    delete_connector_dirpath_later = not os.path.exists(CONNECTOR_FILES_DIRPATH)
    is_server = not args.target
    accept_all_clients = args.accept
    loop = asyncio.get_event_loop()
    cwd = os.getcwd()
        
    class AuthClient:
        #helper for client authentication on server connector
        perform_client_authentication = False
        authenticate = asyncio.Event()
        allow = False
        
        @staticmethod
        def update_allow(status):
            #User chooses the value of "allow", which is sent back to server connector
            AuthClient.allow = status
            AuthClient.perform_client_authentication = False
            AuthClient.authenticate.set()
        
        @staticmethod
        async def authenticate_client(client_name):
            #called as a hook by server when receiving new connection
            #waits for user input
            AuthClient.perform_client_authentication = True
            print(f'Accept client {client_name} ? y/n')
            await AuthClient.authenticate.wait()
            AuthClient.authenticate.clear()
            return AuthClient.allow

    
    if is_server:
        listening_ip = args.bind_server_ip or '0.0.0.0'
        if '.' not in listening_ip:
            listening_ip = aioconnectors.iface_to_ip(listening_ip)
        server_sockaddr = (listening_ip, int(args.port or 0) or aioconnectors.core.Connector.SERVER_ADDR[1])
        if listening_ip == '0.0.0.0':
            listening_addresses = show_up_ips()
        else:
            listening_addresses = [listening_ip]
            
        print(f'Chat Server listening on addresses {listening_addresses[:5]}, and port {server_sockaddr[1]}\n')
        connector_files_dirpath = CONNECTOR_FILES_DIRPATH
        aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=connector_files_dirpath)
        
        def hook_target_directory_any(transport_json):
            #this is just for testing the hook feature
            #this hook is simple and not really needed, we could have used instead :
            #{'target_directory':os.path.join(cwd,'{source_id')})}
            source_id = transport_json['source_id']
            return source_id      
        
        connector_manager = aioconnectors.ConnectorManager(is_server=True, server_sockaddr=server_sockaddr, 
                                                           use_ssl=True, ssl_allow_all=True,
                                                           connector_files_dirpath=connector_files_dirpath, 
                                                           certificates_directory_path=connector_files_dirpath,
                                                           send_message_types=['any'], recv_message_types=['any'], 
                                                           file_recv_config={'any': {'target_directory':cwd}},
                                                           #file_recv_config={'any': {'target_directory':os.path.join(cwd,'{source_id}')}},                                                           
                                                           hook_server_auth_client=None if accept_all_clients else \
                                                           AuthClient.authenticate_client, #)
                                                           hook_target_directory={'any':hook_target_directory_any})
                    
        connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=server_sockaddr, 
                                                   connector_files_dirpath=connector_files_dirpath,
                                                           send_message_types=['any'], recv_message_types=['any'], 
                                                           default_logger_log_level='INFO',
                                                           default_logger_rotate=True)
        destination_id = chat_client_name
    else:
        server_sockaddr = (args.target, args.port or aioconnectors.core.Connector.SERVER_ADDR[1])
        connector_files_dirpath = CONNECTOR_FILES_DIRPATH
        aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=connector_files_dirpath)            
        connector_manager = aioconnectors.ConnectorManager(is_server=False, server_sockaddr=server_sockaddr, 
                                                           use_ssl=True, ssl_allow_all=True,
                                                           connector_files_dirpath=connector_files_dirpath, 
                                                           certificates_directory_path=connector_files_dirpath,
                                                           send_message_types=['any'], recv_message_types=['any'], 
                                                           file_recv_config={'any': {'target_directory':cwd}},
                                                           client_name=chat_client_name, enable_client_try_reconnect=False)

        connector_api = aioconnectors.ConnectorAPI(is_server=False, server_sockaddr=server_sockaddr, 
                                                   connector_files_dirpath=connector_files_dirpath, 
                                                   client_name=chat_client_name,
                                                   send_message_types=['any'], recv_message_types=['any'], 
                                                   default_logger_log_level='INFO',
                                                   default_logger_rotate=True)
        destination_id = None
        
        
    task_manager = loop.create_task(connector_manager.start_connector())
    #run_until_complete now, in order to exit in case of exception
    #for example because of already existing socket
    loop.run_until_complete(task_manager)  
    
    task_recv = task_console = task_send_file = None
    
    async def message_received_cb(logger, transport_json , data, binary):
        #callback when a message is received from peer
        if transport_json.get('await_response'):
            #this response is necessary in args.upload mode, to know when to exit
            #it is in fact used also in chat mode by send_file, even if not mandatory
            loop.create_task(connector_api.send_message(data=data, data_is_json=False, message_type='any', 
                                                        response_id=transport_json['request_id'],
                                                        destination_id=transport_json['source_id']))
        if data:
            #display message received from peer
            print(data.decode())
            print(custom_prompt,end='', flush=True)                

    if not args.upload:  
        task_recv = loop.create_task(connector_api.start_waiting_for_messages(message_type='any', 
                                                                              message_received_cb=message_received_cb))
        
    async def send_file(data, destination_id, with_file, delete_after_upload):
        #upload file to peer. uses await_response always, mandatory for upload mode
        await connector_api.send_message(data=data, data_is_json=False, destination_id=destination_id, 
                                         await_response=True, request_id=random.randint(1,1000),
                                         message_type='any', with_file=with_file)
        if delete_after_upload:
            os.remove(delete_after_upload)
        
    class InputProtocolFactory(asyncio.Protocol):
        #hook user input : sends message to peer, and support special cases (!)

        def connection_made(self, *args, **kwargs):
            super().connection_made(*args, **kwargs)
            print(custom_prompt,end='', flush=True)
            
        def data_received(self, data):
            data = data.decode().strip()
            if AuthClient.perform_client_authentication:
                if data == 'y':
                    AuthClient.update_allow(True)
                else:# data == 'n':
                    AuthClient.update_allow(False)
                print(custom_prompt,end='', flush=True)
                return
            
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
                    
                    data = f'Receiving {upload_path}'
                    with_file={'src_path':upload_path,'dst_type':'any', 'dst_name':os.path.basename(upload_path), 
                               'delete':False}                   
                    loop.create_task(send_file(data, destination_id, with_file, delete_after_upload))
                except Exception as exc:
                    res = str(exc)
                    print(custom_prompt,end='', flush=True)
                    print(res)                        
                print(custom_prompt,end='', flush=True)
                return
            
            if data.startswith('!dezip '):
                try:
                    target = data.split('!dezip ')[1]
                    #copy target to cwd
                    #shutil.copy(os.path.join(CONNECTOR_FILES_DIRPATH, target), target)
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

            loop.create_task(connector_api.send_message(data=data, data_is_json=False, destination_id=destination_id, 
                                                        message_type='any'))                   
            print(custom_prompt,end='', flush=True)
        
    async def connect_pipe_to_stdin(loop, connector_manager):
        #hook user input       
        if not is_server:
            print('Connector waiting to connect ... (Ctrl+C to quit)')

            while True:
                await asyncio.sleep(1)                
                if connector_manager.show_connected_peers():
                    print('Connected !')
                    break                  
        
        transport, protocol = await loop.connect_read_pipe(InputProtocolFactory, sys.stdin)        
    
    async def upload_file(args, destination_id):
        #called when client uses the upload mode, which uploads and disconnects, without opening a chat
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
        #chat mode, hook stdin
        task_console = loop.create_task(connect_pipe_to_stdin(loop, connector_manager))
    else:
        #upload mode, upload and exit
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
    
def show_up_ips():
    #tries to return list of up ipv4 ips (just for display)
    try:
        IPADDR_REGEX = re.compile('(?P<ipaddr>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
        cmd = ['ip', '-4', '-br', '-f', 'inet', 'addr', 'show']
        ifconfig_lines = subprocess.check_output(cmd, encoding='utf8', timeout=5).splitlines()
        addresses = []
        for line in ifconfig_lines:
            if 'UP' in line:
                res = IPADDR_REGEX.search(line)
                if res:
                    addresses.append(res.group('ipaddr'))
        return addresses
    except Exception:
        return []
