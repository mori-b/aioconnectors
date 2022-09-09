import sys
import logging
import json
import argparse

import aioconnectors

logger = logging.getLogger('aioconnectors_main')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

    
HELP = '''
aioconnectors supported commands :
    
    - print_config_templates
    - create_certificates [--ca] [optional dirpath] [--help]
    - replace_server_certificate new_pem_path [optional dirpath] [--revert] [--help]
    - cli (start, stop, restart, show_connected_peers, ignore_peer_traffic, peek_queues, delete_client_certificate, delete_client_token)
    - create_connector <config file path>
    - test_receive_messages <config file path>
    - test_send_messages <config file path>
    - test_publish_messages <config file path>    
    - ping <config file path>
    - chat [--target <server_ip>] [--upload <path>] [--help]
    - --help
    - --version
'''
    
    
if len(sys.argv) > 1:
    if sys.argv[1] == 'create_certificates':
        no_ca = True
        if '--ca' in sys.argv:
            sys.argv.remove('--ca')
            no_ca = False
        if len(sys.argv) == 3:
            if sys.argv[2] == '--help':
                print('create_certificates without argument will create client and server certificates directories '
                      f'under {aioconnectors.core.Connector.CONNECTOR_FILES_DIRPATH}.\n'
                      'You can specify a target directory as an optional argument.\n'
                      '(Use "create_certificates ." to create your target directory in your current working directory.)\n'
                      'Use --no-ca to create certificates for a server having server_ca=False')
                sys.exit(0)
            certificates_directory_path = aioconnectors.helpers.full_path(sys.argv[2])
        else:
            certificates_directory_path = None
        logger.info('Starting create_certificates')            
        res = aioconnectors.ssl_helper.create_certificates(logger, certificates_directory_path=certificates_directory_path,
                                                           no_ca=no_ca)
        if res is False:
            sys.exit(1)

    elif sys.argv[1] == 'replace_server_certificate':
        help_test = 'For server only, replace_server_certificate puts your custom server certificate in the server certificates directories.\n' \
              'First argument (mandatory) : the path of you server pem (the server key should be there too)\n' \
              f'Second argument (optional) : the certificates directory path. If not provided it is : {aioconnectors.core.Connector.CONNECTOR_FILES_DIRPATH}.\n' \
              '(Use "." to create your target directory in your current working directory).\n' \
              'Use "--revert" to put back the original certificates and delete the custom certificate copy.'
        
        if len(sys.argv) == 3:
            if sys.argv[2] == '--help':
                print(help_test)
                sys.exit(0)
            if sys.argv[2] == '--revert':
                res = aioconnectors.ssl_helper.replace_server_certificate(logger, revert=True)
                sys.exit(0)                
            server_certificate_path = aioconnectors.helpers.full_path(sys.argv[2])
            certificates_directory_path = None
        elif len(sys.argv) == 4:
            server_certificate_path = aioconnectors.helpers.full_path(sys.argv[2])            
            certificates_directory_path = aioconnectors.helpers.full_path(sys.argv[3])
        else:
            print(help_test)
            sys.exit(0)
        logger.info('Starting replace_server_certificate')            
        res = aioconnectors.ssl_helper.replace_server_certificate(logger, server_certificate_path=server_certificate_path,
                                                        certificates_directory_path=certificates_directory_path)
        if res is False:
            sys.exit(1)
        
    elif sys.argv[1] == 'print_config_templates':
        Connector = aioconnectors.core.Connector
    
        manager_config_template = dict(default_logger_log_level='INFO', default_logger_rotate=True,
                        default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH,
                        is_server=True, server_sockaddr=Connector.SERVER_ADDR, reuse_server_sockaddr=False,
                        reuse_uds_path_commander_server=False, reuse_uds_path_send_to_connector=False,
                        use_ssl=Connector.USE_SSL, ssl_allow_all=False, use_token=Connector.USE_TOKEN,
                        server_ca=Connector.SERVER_CA, server_ca_certs_not_stored=True, server_secure_tls=True,
                        certificates_directory_path=Connector.CONNECTOR_FILES_DIRPATH,
                        tokens_directory_path=Connector.CONNECTOR_FILES_DIRPATH,                        
                        client_name=None, client_bind_ip=None, 
                        send_message_types=Connector.DEFAULT_MESSAGE_TYPES, 
                        recv_message_types=Connector.DEFAULT_MESSAGE_TYPES,
                        subscribe_message_types=[], pubsub_central_broker=False,
                        disk_persistence_send=Connector.DISK_PERSISTENCE_SEND, 
                        disk_persistence_recv=Connector.DISK_PERSISTENCE_RECV, 
                        max_size_persistence_path=Connector.MAX_SIZE_PERSISTENCE_PATH,
                        file_recv_config={}, debug_msg_counts=Connector.DEBUG_MSG_COUNTS, silent=Connector.SILENT, 
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, 
                        uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                        enable_client_try_reconnect=True, keep_alive_period=None, keep_alive_timeout=Connector.KEEP_ALIVE_TIMEOUT,
                        max_number_of_unanswered_keep_alive=Connector.MAX_NUMBER_OF_UNANSWERED_KEEP_ALIVE,
                        connect_timeout = Connector.CONNECT_TIMEOUT,
                        send_timeout=Connector.SEND_TIMEOUT, max_size_file_upload_send=Connector.MAX_SIZE_FILE_UPLOAD_SEND,
                        max_size_file_upload_recv=Connector.MAX_SIZE_FILE_UPLOAD_RECV, max_certs=Connector.MAX_CERTS,
                        everybody_can_send_messages=Connector.EVERYBODY_CAN_SEND_MESSAGES, send_message_types_priorities={},
                        proxy={}, alternate_client_default_cert=Connector.ALTERNATE_CLIENT_DEFAULT_CERT,
                        blacklisted_clients_id=None, blacklisted_clients_ip=None, blacklisted_clients_subnet=None,
                        whitelisted_clients_id=None, whitelisted_clients_ip=None, whitelisted_clients_subnet=None,
                        ignore_peer_traffic=False, client_cafile_verify_server=None,
                        token_verify_peer_cert=Connector.TOKEN_VERIFY_PEER_CERT,
                        token_client_send_cert=Connector.TOKEN_CLIENT_SEND_CERT,
                        token_client_verify_server_hostname=Connector.TOKEN_CLIENT_VERIFY_SERVER_HOSTNAME,
                        token_server_allow_authorized_non_default_cert=False)
                        
        print('\n- MANAGER TEMPLATE, used to create a connector')
        print(json.dumps(manager_config_template, indent=4, sort_keys=True))
        file_recv_config = {'any': {'target_directory':'/var/tmp/aioconnectors/{message_type}/{source_id}/',
                                    'owner':'user:user', 'override_existing':False}}
        print('\n- file_recv_config example, used inside MANAGER TEMPLATE')
        print(json.dumps(file_recv_config, indent=4, sort_keys=True))                
        
        api_config_template = dict(default_logger_log_level='INFO', default_logger_rotate=True,
                        default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                        is_server=True, server_sockaddr=Connector.SERVER_ADDR, client_name=None,
                        uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET, 
                        uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                        send_message_types=Connector.DEFAULT_MESSAGE_TYPES, 
                        recv_message_types=Connector.DEFAULT_MESSAGE_TYPES,
                        receive_from_any_connector_owner=True, pubsub_central_broker=False)                 
        print('\n- API TEMPLATE, used to send/receive messages')
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

    elif sys.argv[1] == 'test_publish_messages':
        if len(sys.argv) != 3:
            print('Usage : test_publish_messages <config file path>')
            sys.exit(1)
        if sys.argv[2] == '--help':
            print('Usage : test_publish_messages <config file path>')
            sys.exit(0)
        config_file_path=sys.argv[2]            
        aioconnectors.applications.test_publish_messages(config_file_path, logger)
                
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
        #inside chat, prepend "!" to call a local shell command, !exit" to exit, "!upload <path>" to upload to cwd, 
        #"!dezip" to unzip an uploaded file.
        print('\nWelcome to aioconnectors chat !')
        print('Usage :\n- Type messages, or !exit to exit, or any shell command preceded by a ! to execute locally\n'
              '- !upload <file or dir path> to upload to peer\'s current working directory\n'
              '- !dezip <downloaded file name> to unzip a file\n')
        parser = argparse.ArgumentParser()
        parser.add_argument('chat')
        parser.add_argument('--target', nargs='?', default=None, help="server ip, mandatory for client")
        parser.add_argument('--accept', action='store_true', help="accept all clients if specified, optional for server")        
        parser.add_argument('--port', nargs='?', default=None, help="server port, optional for server and client")
        parser.add_argument('--bind_server_ip', nargs='?', default=None, help="bind to ip, optional for server")        
        parser.add_argument('--upload', nargs='?', default=False, help="path of directory or file to upload")
        parser.add_argument('--nowrap', action='store_true', help="disable tab completion")        
        
        args = parser.parse_args()
        aioconnectors.applications.chat(args)
                        
    elif sys.argv[1] == '--help':
        print(HELP)
    elif sys.argv[1] == '--version':
        print(aioconnectors.__version__)
    else:
        print('Unknown command : '+str(sys.argv[1]))
else:
    print(HELP)
