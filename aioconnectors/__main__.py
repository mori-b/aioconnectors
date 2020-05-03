import sys
import os
import logging
import json
import argparse

import aioconnectors

logger = logging.getLogger('aioconnectors_main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

def full_path(the_path):
    return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))    
    
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
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                        enable_client_try_reconnect=True)
        print('\nMANAGER TEMPLATE, used to create a connector')
        print(json.dumps(manager_config_template, indent=4, sort_keys=True))
                
        api_config_template = dict(default_logger_log_level='INFO', default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        is_server=True, server_sockaddr=Connector.SERVER_ADDR, client_name=None,
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                        send_message_types=Connector.DEFAULT_MESSAGE_TYPES, recv_message_types=Connector.DEFAULT_MESSAGE_TYPES)                 
        print('\nAPI TEMPLATE, used to send/receive messages')
        print(json.dumps(api_config_template, indent=4, sort_keys=True))

    elif sys.argv[1] == 'create_connector':
        if len(sys.argv) != 3:
            print('Usage : create_connector <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : create_connector <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        aioconnectors.applications.create_connector(config_file_path, logger)        

    elif sys.argv[1] == 'cli':
        aioconnectors.applications.cli(logger)

    elif sys.argv[1] == 'test_receive_messages':
        if len(sys.argv) != 3:
            print('Usage : test_receive_messages <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : test_receive_messages <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]       
        aioconnectors.applications.test_receive_messages(config_file_path, logger)

    elif sys.argv[1] == 'test_send_messages':
        if len(sys.argv) != 3:
            print('Usage : test_send_messages <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : test_send_messages <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        aioconnectors.applications.test_send_messages(config_file_path, logger)
                
    elif sys.argv[1] == 'ping':
        if len(sys.argv) != 3:
            print('Usage : ping <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : ping <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]
        aioconnectors.applications.ping(config_file_path, logger)

    elif sys.argv[1] == 'chat':
        #usage
        #python3 -m aioconnectors chat
        #python3 -m aioconnectors chat --target 127.0.0.1 [--upload <path>]
        #inside chat, possible to type "!exit" to exit, and "!upload <path>" to upload
        print('\nWelcome to aioconnectors chat !')
        print('Usage :\n- Type messages, or !exit to exit, or any shell command preceded by a ! to execute locally\n- !upload <file or dir path> to upload to peer\'s /tmp/aioconnectors\n- !import <downloaded file name> to import a received file from /tmp/aioconnectors to cwd\n- !zimport <downloaded file name> to import and unzip it\n')
        parser = argparse.ArgumentParser()
        parser.add_argument('chat')
        parser.add_argument('--target', nargs='?', default=None, help="server ip, mandatory for client")
        parser.add_argument('--port', nargs='?', default=None, help="server port, optional for server and client")
        parser.add_argument('--bind_server_ip', nargs='?', default=None, help="bind to ip, optional for server")        
        parser.add_argument('--upload', nargs='?', default=False, help="path of directory or file to upload")
        args = parser.parse_args()
        aioconnectors.applications.chat(args)
                        
    elif sys.argv[1] == '--help':
        print(HELP)
    else:
        print('Unknown command : '+str(sys.argv[1]))
else:
    print(HELP)
