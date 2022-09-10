import asyncio
import json
import logging
import socket
import weakref
import os
import ssl
import uuid
import re
import ipaddress
import stat
from copy import deepcopy
from time import time
from base64 import b64encode
import hashlib

from .connection import FullDuplex, DEBUG_SHOW_DATA, MessageFields, Structures
from .helpers import full_path, chown_nobody_permissions, iface_to_ip, get_tmp_dir, PYTHON_GREATER_37
from .ssl_helper import SSL_helper


class Connector:
    ############################################
    #default values configurable at __init__
    SERVER_ADDR =  ('127.0.0.1',10673)
    USE_SSL, USE_TOKEN, SSL_ALLOW_ALL, SERVER_CA = True, False, False, False
    CONNECTOR_FILES_DIRPATH = get_tmp_dir()
    DISK_PERSISTENCE_SEND = False    #can be boolean, or list of message types having disk persistence enabled
    #RAM_PERSISTENCE cannot be true in the current implementation, since queue_send[peername] doesn't exist anymore in disconnected mode
    #however the code still exists partially, in case of future change
    PERSISTENCE_SEND_FILE_NAME = 'connector_disk_persistence_send_from_{}_to_peer'
    DISK_PERSISTENCE_RECV = False
    PERSISTENCE_RECV_FILE_NAME = 'connector_disk_persistence_recv_by_{}'    
    MAX_SIZE_FILE_UPLOAD_SEND = 8_589_930_194 #8gb
    MAX_SIZE_FILE_UPLOAD_RECV = 8_589_930_194 #8gb    
    MAX_SIZE_CHUNK_UPLOAD = 1_073_741_824 #1gb    
    MAX_SIZE_PERSISTENCE_PATH = 1_073_741_824 #1gb
    READ_CHUNK_SIZE = 104_857_600 #100mb    
    READ_PERSISTENCE_CHUNK_SIZE = 1024
    UDS_PATH_RECEIVE_PRESERVE_SOCKET = True
    UDS_PATH_SEND_PRESERVE_SOCKET = True    
    SILENT=True    
    MAX_CERTS = 1024
    #USE_ACK = False  #or can be a list of message_types like ['type1']    
    DEBUG_MSG_COUNTS = True
    TOKEN_VERIFY_PEER_CERT = True
    SEND_TIMEOUT = 50
    
    #client only
    KEEP_ALIVE_TIMEOUT = 5  
    MAX_NUMBER_OF_UNANSWERED_KEEP_ALIVE = 2
    ALTERNATE_CLIENT_DEFAULT_CERT = False    
    TOKEN_CLIENT_SEND_CERT = True
    TOKEN_CLIENT_VERIFY_SERVER_HOSTNAME = None
    CONNECT_TIMEOUT = 10    #can take time to resolve url

    ############################################    
    #default values of internals (not configurable at __init__)
    SLEEP_BETWEEN_START_FAILURES = 5 #10
    ASYNC_TIMEOUT = 10
    MAX_QUEUE_SIZE = 300_000 #1_000_000 #8192
    AWAIT_RESPONSE_TIMEOUT = 3600 #1 hour
    MAX_NUMBER_OF_AWAITING_REQUESTS = 1000
    MAX_SOCKET_BUFFER_SIZE = 2 ** 16    #2 ** 16 is asyncio default
    MAX_TRANSPORT_ID = 100_000
    MAX_RETRIES_BEFORE_ACK = 3
    DEFAULT_MESSAGE_TYPES = ['any']   
    PERSISTENCE_SEPARATOR = b'@@@PERSISTENCE_SEPARATOR@@@'
    PERSISTENCE_SEPARATOR_REPLACEMENT = b'#@@PERSISTENCE_SEPARATOR@@#'    
    #FILE_RECV_CONFIG example : {'any': {'target_directory':'/var/tmp/aioconnectors/{message_type}/{source_id}/',
    #'owner':'user:user', 'override_existing':False}}
    FILE_RECV_CONFIG = {}    
    DELETE_CLIENT_PRIVATE_KEY_ON_SERVER = False
    EVERYBODY_CAN_SEND_MESSAGES = True
    DEFAULT_ACTIVE_CONNECTORS_NAME = 'active_connectors.json'    
    UDS_PATH_COMMANDER = 'uds_path_commander_{}'   
    
    UDS_PATH_SEND_TO_CONNECTOR_SERVER = 'uds_path_send_to_connector_server_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER = 'uds_path_receive_from_connector_server_{}_{}'    

    UDS_PATH_SEND_TO_CONNECTOR_CLIENT = 'uds_path_send_to_connector_client_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT = 'uds_path_receive_from_connector_client_{}_{}'    
    MAX_LENGTH_UDS_PATH = 104
    SUPPORT_PRIORITIES = True
    MAX_TOKEN_LENGTH = 64
    MAX_NUMBER_OF_TOKENS = 10000
    
    #client only
    KEEP_ALIVE_CLIENT_REQUEST_ID = 'keep_alive_client_check'

    
    def __init__(self, logger, server_sockaddr=SERVER_ADDR, is_server=True, client_name=None, client_bind_ip=None,
                 use_ssl=USE_SSL, ssl_allow_all=SSL_ALLOW_ALL, use_token=USE_TOKEN, certificates_directory_path=CONNECTOR_FILES_DIRPATH, 
                 server_ca=SERVER_CA, server_ca_certs_not_stored=True, server_secure_tls=True,
                 tokens_directory_path=CONNECTOR_FILES_DIRPATH, disk_persistence_send=DISK_PERSISTENCE_SEND,
                 disk_persistence_recv=DISK_PERSISTENCE_RECV, max_size_persistence_path=MAX_SIZE_PERSISTENCE_PATH, #use_ack=USE_ACK,
                 send_message_types=None, recv_message_types=None, subscribe_message_types=None,
                 tool_only=False, file_recv_config=None, config_file_path=None,
                 debug_msg_counts=DEBUG_MSG_COUNTS, silent=SILENT, connector_files_dirpath = CONNECTOR_FILES_DIRPATH,
                 uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET,
                 uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET,
                 hook_server_auth_client=None, hook_target_directory=None, hook_allow_certificate_creation=None,
                 hook_proxy_authorization=None, enable_client_try_reconnect=True, connect_timeout=CONNECT_TIMEOUT,
                 keep_alive_period=None, keep_alive_timeout=KEEP_ALIVE_TIMEOUT, send_timeout=SEND_TIMEOUT,
                 max_number_of_unanswered_keep_alive=MAX_NUMBER_OF_UNANSWERED_KEEP_ALIVE,
                 reuse_server_sockaddr=False, reuse_uds_path_send_to_connector=False, reuse_uds_path_commander_server=False,
                 max_size_file_upload_send=MAX_SIZE_FILE_UPLOAD_SEND, max_size_file_upload_recv=MAX_SIZE_FILE_UPLOAD_RECV,
                 everybody_can_send_messages=EVERYBODY_CAN_SEND_MESSAGES, max_certs=MAX_CERTS,
                 send_message_types_priorities=None, pubsub_central_broker=False, proxy=None,
                 alternate_client_default_cert=ALTERNATE_CLIENT_DEFAULT_CERT,
                 blacklisted_clients_id=None, blacklisted_clients_ip=None, blacklisted_clients_subnet=None,
                 whitelisted_clients_id=None, whitelisted_clients_ip=None, whitelisted_clients_subnet=None,
                 hook_whitelist_clients=None, ignore_peer_traffic=False,
                 hook_store_token=None, hook_load_token=None, client_cafile_verify_server=None,
                 token_verify_peer_cert=TOKEN_VERIFY_PEER_CERT, token_client_send_cert=TOKEN_CLIENT_SEND_CERT,
                 token_client_verify_server_hostname=TOKEN_CLIENT_VERIFY_SERVER_HOSTNAME,
                 token_server_allow_authorized_non_default_cert=False
):                 
        
        self.logger = logger.getChild('server' if is_server else 'client')
        if tool_only:
            self.logger = self.logger.getChild('api')
                    
        try:
            self.is_running = False        
            self.tool_only = tool_only
            
            self.debug_msg_counts = debug_msg_counts
            #self.use_ack = use_ack
            self.connector_files_dirpath = full_path(connector_files_dirpath)
            if not os.path.isdir(self.connector_files_dirpath):
                os.makedirs(self.connector_files_dirpath)
    
            self.uds_path_receive_preserve_socket = uds_path_receive_preserve_socket
            self.uds_path_send_preserve_socket = uds_path_send_preserve_socket
            self.silent = silent
            if self.debug_msg_counts:
                self.msg_counts = {'store_persistence_send':0, 'load_persistence_send':0, 'send_no_persist':0,
                                   'queue_sent':0,'queue_recv':0, 'msg_recvd_uds':0, 'store_persistence_recv':0,
                                   'load_persistence_recv':0}
                self.previous_msg_counts = deepcopy(self.msg_counts)
            
            self.server_sockaddr = server_sockaddr
            self.is_server = is_server            
            self.use_ssl, self.ssl_allow_all, self.use_token = use_ssl, ssl_allow_all, use_token
            self.server_ca, self.server_secure_tls = server_ca, server_secure_tls
            self.server_ca_certs_not_stored = server_ca_certs_not_stored
            self.certificates_directory_path = full_path(certificates_directory_path)
            self.tokens_directory_path = full_path(tokens_directory_path)
            if self.tokens_directory_path:
                if not os.path.exists(self.tokens_directory_path):
                    os.makedirs(self.tokens_directory_path)
            self.server = self.send_to_connector_server = None

            self.reuse_server_sockaddr = reuse_server_sockaddr
            self.reuse_uds_path_send_to_connector = reuse_uds_path_send_to_connector
            self.reuse_uds_path_commander_server = reuse_uds_path_commander_server
            self.hook_target_directory = hook_target_directory
            self.hook_allow_certificate_creation = hook_allow_certificate_creation
            self.hook_proxy_authorization = hook_proxy_authorization
            self.hook_store_token, self.hook_load_token = hook_store_token, hook_load_token
            self.token_verify_peer_cert = token_verify_peer_cert
            self.client_cafile_verify_server = client_cafile_verify_server
            self.max_certs = max_certs
            self.send_timeout = send_timeout
            self.config_file_path = config_file_path
            
            if self.is_server:
                self.source_id = str(self.server_sockaddr)
                if self.server_secure_tls:                
                    self.logger.info('Server allowing only clients with TLS version >= v1.2')    
                self.logger.info('Server has source id : '+self.source_id)                
                self.alnum_source_id = '_'.join([self.alnum_name(el) for el in self.source_id.split()])
                self.alnum_source_id_for_uds = self.limit_length_for_uds(self.alnum_source_id)
                self.tokens_file_path = os.path.join(self.tokens_directory_path or '', 'server_tokens.json')
                self.token_server_allow_authorized_non_default_cert = token_server_allow_authorized_non_default_cert
                
                self.hook_server_auth_client = hook_server_auth_client
                if hook_server_auth_client:
                    self.logger.info(f'Connector Server {self.source_id} has a hook_server_auth_client')
                if hook_target_directory:
                    self.logger.info(f'Connector Server {self.source_id} has a hook_target_directory')
                if hook_allow_certificate_creation:
                    self.logger.info(f'Connector Server {self.source_id} has a hook_allow_certificate_creation')

                if send_message_types is None:
                    send_message_types = self.DEFAULT_MESSAGE_TYPES
                if recv_message_types is None:
                    recv_message_types = self.DEFAULT_MESSAGE_TYPES
                self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
                
                self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath,
                                                               self.UDS_PATH_SEND_TO_CONNECTOR_SERVER.format(self.alnum_source_id_for_uds))
                if len(self.uds_path_send_to_connector) > self.MAX_LENGTH_UDS_PATH:
                    raise Exception(f'{self.uds_path_send_to_connector} is longer than {self.MAX_LENGTH_UDS_PATH}')
                    
                self.uds_path_receive_from_connector = {}
                for recv_message_type in self.recv_message_types:
                    self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath,
                        self.UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER.format(recv_message_type, self.alnum_source_id_for_uds))
                    if len(self.uds_path_receive_from_connector[recv_message_type]) > self.MAX_LENGTH_UDS_PATH:
                        raise Exception(f'{self.uds_path_receive_from_connector[recv_message_type]} is longer '
                                           f'than {self.MAX_LENGTH_UDS_PATH}')
                
            else:
                self.client_bind_ip = client_bind_ip
                self.source_id = client_name
                self.logger.info('Client has source id : '+self.source_id)   
                if hook_target_directory:
                    self.logger.info(f'Connector Client {self.source_id} has a hook_target_directory')               
                if hook_store_token:
                    self.logger.info(f'Connector Client {self.source_id} has a hook_store_token')               
                if hook_load_token:
                    self.logger.info(f'Connector Client {self.source_id} has a hook_load_token')               
                    
                self.alnum_source_id = self.alnum_name(self.source_id)
                self.alnum_source_id_for_uds = self.limit_length_for_uds(self.alnum_source_id)
                self.tokens_file_path = os.path.join(self.tokens_directory_path or '', self.alnum_name(self.source_id))
                self.enable_client_try_reconnect = enable_client_try_reconnect
                self.proxy = proxy or {}
                self.inside_end_sockpair = None
                self.token_client_send_cert = token_client_send_cert
                self.token_client_verify_server_hostname = token_client_verify_server_hostname                
                
                if self.proxy.get('enabled'):
                    #proxy can be like {'enabled':True, 'address':'1.2.3.4 or bla.com', 'port':'22',
                    #'ssl_server':False, 'authorization':''},
                    #'authorization' can also be like : {'username':<username>, 'password':<password>}
                    self.logger.info(f'Client {self.source_id} will use proxy {str(self.proxy)}')
                    if hook_proxy_authorization:
                        self.logger.info(f'Connector Client {self.source_id} has a hook_proxy_authorization')
                
                if send_message_types is None:
                    send_message_types = self.DEFAULT_MESSAGE_TYPES
                if recv_message_types is None:
                    recv_message_types = self.DEFAULT_MESSAGE_TYPES
                if subscribe_message_types is None:
                    subscribe_message_types = []
                self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
                self.subscribe_message_types = subscribe_message_types
                
                self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath,
                                                    self.UDS_PATH_SEND_TO_CONNECTOR_CLIENT.format(self.alnum_source_id_for_uds))
                if len(self.uds_path_send_to_connector) > self.MAX_LENGTH_UDS_PATH:
                    raise Exception(f'{self.uds_path_send_to_connector} is longer than {self.MAX_LENGTH_UDS_PATH}')
                
                self.uds_path_receive_from_connector = {}           
                for recv_message_type in self.recv_message_types:
                    self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath,
                            self.UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT.format(recv_message_type, self.alnum_source_id_for_uds))
                    if len(self.uds_path_receive_from_connector[recv_message_type]) > self.MAX_LENGTH_UDS_PATH:
                        raise Exception(f'{self.uds_path_receive_from_connector[recv_message_type]} is longer '
                                           f'than {self.MAX_LENGTH_UDS_PATH}')
            
            self.commander_server = self.commander_server_task = None
            self.uds_path_commander = os.path.join(self.connector_files_dirpath,
                                                   self.UDS_PATH_COMMANDER.format(self.alnum_source_id_for_uds))
            if len(self.uds_path_commander) > self.MAX_LENGTH_UDS_PATH:
                raise Exception(f'{self.uds_path_commander} is longer than {self.MAX_LENGTH_UDS_PATH}')
                    
            if not tool_only:
                self.active_connectors_path = os.path.join(self.connector_files_dirpath, self.DEFAULT_ACTIVE_CONNECTORS_NAME)                
                self.full_duplex_connections = {}
                if self.is_server:
                    self.blacklisted_clients_id = set(blacklisted_clients_id or [])
                    self.blacklisted_clients_ip = set(blacklisted_clients_ip or [])      
                    self.blacklisted_clients_subnet = set(blacklisted_clients_subnet or [])
                    self.whitelisted_clients_id = set(whitelisted_clients_id or [])
                    self.whitelisted_clients_ip = set(whitelisted_clients_ip or [])      
                    self.whitelisted_clients_subnet = set(whitelisted_clients_subnet or [])
                    self.hook_whitelist_clients = hook_whitelist_clients
                else:
                    self.client_certificate_name = None
                    self.keep_alive_period = keep_alive_period
                    self.keep_alive_timeout = keep_alive_timeout
                    self.max_number_of_unanswered_keep_alive = max_number_of_unanswered_keep_alive 
                    self.alternate_client_default_cert = alternate_client_default_cert
                    self.alternate_client_cert_toggle_default = False
                    self.client_reconnect_last_timestamp = 0
                    self.connect_timeout = connect_timeout
                
                if self.use_ssl:                    
                    self.ssl_helper = SSL_helper(self.logger, self.is_server, certificates_directory_path=self.certificates_directory_path,
                                                 max_certs=self.max_certs, server_ca=self.server_ca,
                                                 server_ca_certs_not_stored=self.server_ca_certs_not_stored)
                    self.logger.info(f'Connector will use ssl, with ssl_allow_all : {ssl_allow_all}, and server_ca : {server_ca},'
                                     f' with certificates directory : {self.ssl_helper.certificates_base_path},'
                                     f' and with client_cafile_verify_server : {client_cafile_verify_server}')
                    
                    #this code is used instead in run_client since the alternate_client_cert_toggle_default mechanism
                    #if self.is_server:                
                    #    pass
                    #elif not self.ssl_allow_all:
                    #    if os.path.exists(self.ssl_helper.CLIENT_PEM_PATH.format(self.source_id)):
                    #        self.client_certificate_name = self.source_id
                    #        self.logger.info('Client will use a unique certificate : '+self.client_certificate_name)                
                    #    else:
                    #        self.client_certificate_name = self.ssl_helper.CLIENT_DEFAULT_CERT_NAME
                    #        self.logger.info('Client will use the default certificate : '+self.client_certificate_name)
                            
                    if self.ssl_allow_all:                        
                        self.logger.info(f'Connector will use token_verify_peer_cert : {token_verify_peer_cert}, '
                                     f'token_client_send_cert : {token_client_send_cert}, '
                                     f'token_client_verify_server_hostname : {token_client_verify_server_hostname}')
                else:
                    self.logger.info('Connector will not use ssl')
                    
                if self.use_token:          
                    self.logger.info('Connector will use tokens, with tokens file path '+self.tokens_file_path)
                    if not self.use_ssl:
                        self.logger.warning('Connector cannot use tokens without use_ssl, not using tokens')
                        self.use_token = None
                    else:
                        if not self.ssl_allow_all:
                            self.logger.warning('Connector cannot use tokens without ssl_allow_all, not using tokens')
                            self.use_token = None
                        else:
                            if self.is_server:
                                self.tokens = self.load_server_tokens()
                            else:
                                self.token = self.load_client_token()
                else:                        
                    self.logger.info('Connector will not use tokens')
                    
                    
                self.everybody_can_send_messages = everybody_can_send_messages
                self.send_message_types_priorities = send_message_types_priorities
                if not self.send_message_types_priorities:
                    self.send_message_types_priorities = {}
                self.pubsub_central_broker = pubsub_central_broker if self.is_server else False
                self.max_size_file_upload_send = max_size_file_upload_send
                self.max_size_file_upload_recv = max_size_file_upload_recv
                self.disk_persistence = disk_persistence_send
                self.persistence_path = os.path.join(self.connector_files_dirpath,
                                            self.PERSISTENCE_SEND_FILE_NAME.format(self.alnum_source_id) + '_') #peer name will be appended
                self.disk_persistence_recv = disk_persistence_recv
                self.persistence_recv_path = os.path.join(self.connector_files_dirpath,
                                            self.PERSISTENCE_RECV_FILE_NAME.format(self.alnum_source_id) + '_') #msg type will be appended         
                self.ram_persistence = False #ram_persistence    
                self.max_size_persistence_path = max_size_persistence_path
                if self.disk_persistence:
                    if self.persistence_path:
                        self.logger.info('Connector will use send persistence path {} with max '
                                         'size {}'.format(self.persistence_path, self.max_size_persistence_path))
                    else:
                        self.logger.warning('Connector misconfigured : disk_persistence is enabled without persistence_path send')
                        #self.logger.info('Connector will use persistence ram')
                else:
                    self.logger.info('Connector will not use persistence send')                                

                if self.disk_persistence_recv:
                    if self.persistence_recv_path:
                        self.logger.info('Connector will use recv persistence path {} with max '
                                         'size {}'.format(self.persistence_recv_path, self.max_size_persistence_path))
                    else:
                        self.logger.warning('Connector misconfigured : disk_persistence is enabled without persistence_path recv')
                        #self.logger.info('Connector will use persistence ram')
                else:
                    self.logger.info('Connector will not use persistence recv')                                
                    
                #if self.disk_persistence:
                    #self.delete_previous_persistence_remains()
                
                #messages_awaiting_response looks like {message_type: {peername: {'request_id': asyncio.Event()}}}
                self.messages_awaiting_response = {'_ping':{}} 
                for message_type in self.send_message_types:
                    self.messages_awaiting_response[message_type] = {}
                
                if file_recv_config is None:
                    self.file_recv_config = self.FILE_RECV_CONFIG
                else:
                    self.file_recv_config = file_recv_config
                    
                self.ignore_peer_traffic = ignore_peer_traffic
                self.loop = asyncio.get_event_loop()
                #commander_server lives besides start/stop
                if os.path.exists(self.uds_path_commander) and not self.reuse_uds_path_commander_server:
                    raise Exception(f'{self.uds_path_commander} already exists. Cannot create_commander_server')         
                self.commander_server_task = self.loop.create_task(self.create_commander_server())
                    
        except Exception:
            self.logger.exception('init')
            raise

    def update_config_file(self, kwargs):
        if kwargs and self.config_file_path:
            try:
                with open(self.config_file_path, 'r') as fd:
                    config_file = json.load(fd)
                for key, value in kwargs.items():
                    config_file[key] = value
                with open(self.config_file_path, 'w') as fd:
                    json.dump(config_file, fd, indent=4, sort_keys=True)
                return True
            except Exception:
                self.logger.exception('update_config_file')
                return False
        return True

    def load_server_tokens(self):
        try:
            if not os.path.exists(self.tokens_file_path):
                with open(self.tokens_file_path, 'w') as fd:
                    fd.write('{}')
                    return {}
            with open(self.tokens_file_path, 'r') as fd:
                res = json.load(fd)
            self.tokens = res
            return res
        except Exception:
            self.logger.exception('load_server_tokens')
            return {}
        
    def store_server_tokens(self, tokens_dict):
        self.logger.info(f'{self.source_id} Calling store_server_tokens')
        if len(tokens_dict) > self.MAX_NUMBER_OF_TOKENS:
            self.logger.warning(f'{self.source_id} store_server_tokens with too many token : {len(tokens_dict)}, aborting')
            return
        self.tokens = tokens_dict
        with open(self.tokens_file_path, 'w') as fd:
            json.dump(tokens_dict, fd, indent=4, sort_keys=True)
        os.chmod(self.tokens_file_path, stat.S_IRUSR | stat.S_IWUSR)        

    def load_client_token(self):
        if not os.path.exists(self.tokens_file_path):
            return None        
        with open(self.tokens_file_path, 'r') as fd:     
            res = fd.read()
        if self.hook_load_token:
            res = self.hook_load_token(res)           
        self.token = res[:self.MAX_TOKEN_LENGTH]
        return res

    def store_client_token(self, token):
        self.token = token[:self.MAX_TOKEN_LENGTH]
        if self.hook_store_token:
            self.logger.info(f'{self.source_id} Calling store_client_token with hook_store_token')
            token = self.hook_store_token(token)
        else:
            self.logger.info(f'{self.source_id} Calling store_client_token')            
        with open(self.tokens_file_path, 'w') as fd:
            fd.write(token)
        os.chmod(self.tokens_file_path, stat.S_IRUSR | stat.S_IWUSR)        
        
    async def create_commander_server(self):
        if os.path.exists(self.uds_path_commander) and not self.reuse_uds_path_commander_server:
            self.logger.exception('create_commander_server!')
            raise Exception(f'{self.uds_path_commander} already exists. Cannot create_commander_server')  
        self.logger.info('Calling create_commander_server')
        self.commander_server = await asyncio.start_unix_server(self.commander_cb, path=self.uds_path_commander)
        #chown_nobody_permissions(self.uds_path_commander, self.logger)
        try:
            #only connector user/group can send commands to connector
            os.chmod(self.uds_path_commander, stat.S_IRWXU | stat.S_IRWXG)
        except Exception:
            self.logger.exception(f'{self.source_id} could not set permissions for {self.uds_path_commander}')
        
                
    async def log_msg_counts(self):
        while True:
            await asyncio.sleep(2)
            if self.msg_counts != self.previous_msg_counts:
            #if any(self.msg_counts.values()):
                if not self.silent:
                    print(self.msg_counts)            
                self.logger.info('msg_counts : '+str(self.msg_counts))
                self.previous_msg_counts = deepcopy(self.msg_counts)
            queues_stats = self.peek_queues()
            queues_stats_display = {}            
            if queues_stats['queue_recv']:
                queues_stats_display['queue_recv'] = queues_stats['queue_recv']                
            if any(queues_stats['queue_send'].values()):
                queues_stats_display['queue_send'] = {key:value for (key,value) in \
                                            queues_stats['queue_send'].items() if value}
            if queues_stats_display:
                self.logger.info('peek_queues : '+json.dumps(queues_stats_display, indent=4, sort_keys=True))
                
    def alnum_name(self, name, limit_length=False):
        res = ''.join([str(letter) for letter in name if str(letter).isalnum()])
        return res
                
    def limit_length_for_uds(self, name):
        if len(name) > 32:
            name = hashlib.md5(name.encode()).hexdigest()
        return name
    
    async def start(self, connector_socket_only=False, alternate_client_cert_toggle_default=False):
        #connector_socket_only is used by client_wait_for_reconnect
        #alternate_client_cert_toggle_default is used only by client_wait_for_reconnect
        if self.is_running:
            self.logger.warning('Connector is already running, ignoring start...')
            return
        self.is_running = True
        self.logger.info(f'{self.source_id} Connector starting ...')
        try:
            if not connector_socket_only:
                self.queue_send = {}    #key=peername, value=asyncio.Queue
                self.queue_send_transition_to_connect = {}
                self.queue_recv = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)                
                self.tasks = {}    #key=task name            
                self.reader_writer_uds_path_receive = {}
                if self.debug_msg_counts:
                    self.tasks['log_msg_counts'] = self.loop.create_task(self.log_msg_counts())
                #these tasks are created once at start
                #they should be unchanged during the eventual restarts with connector_socket_only
                self.tasks['queue_recv_from_connector'] = self.loop.create_task(self.queue_recv_from_connector())
                self.tasks['queue_send_to_connector'] = self.loop.create_task(self.queue_send_to_connector())                
            
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.reuse_server_sockaddr:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
            
            if self.is_server:
                if self.use_token:                    
                    self.logger.info('Connector server will use tokens, with tokens file path '+self.tokens_file_path)
                    self.tokens = self.load_server_tokens()
                else:                        
                    self.logger.info('Connector server will not use tokens')
                
                self.sock.setblocking(False)
                sock_bind = self.server_sockaddr
                if '.' not in sock_bind[0]:
                    self.logger.info(f'Trying to bind {self.server_sockaddr} with the correct ip')
                    sock_bind = (iface_to_ip(sock_bind[0]), sock_bind[1])
                    self.logger.info(f'Binding {self.server_sockaddr} using {sock_bind}')                    
                self.sock.bind(sock_bind)
                self.tasks['run_server'] = self.loop.create_task(self.run_server())
            else:
                if self.use_token:                    
                    self.logger.info('Connector client will use tokens, with tokens file path '+self.tokens_file_path)
                    if alternate_client_cert_toggle_default:
                        self.token = None
                        self.logger.info('Client alternating with get_new_token')
                    else:
                        self.token = self.load_client_token()
                else:                        
                    self.logger.info('Connector client will not use tokens')
                
                self.sock.setblocking(False)                
                if self.client_bind_ip:
                    sock_bind = self.client_bind_ip
                    if not '.' in sock_bind:
                        self.logger.info(f'Trying to bind {self.client_bind_ip} with a correct ip')
                        sock_bind = iface_to_ip(sock_bind)
                        self.logger.info(f'Binding {self.client_bind_ip} using {sock_bind}')                         
                    self.sock.bind((sock_bind,0))
                server_sockaddr_addr = self.server_sockaddr[0]
                if '.' not in server_sockaddr_addr:
                    #this is useful only when client and server are on the same machine
                    server_sockaddr_addr = iface_to_ip(server_sockaddr_addr)
                    
                if not self.proxy.get('enabled'):
                    await asyncio.wait_for(self.loop.sock_connect(self.sock,
                                                (server_sockaddr_addr, self.server_sockaddr[1])), timeout=self.connect_timeout)                                
                    self.logger.info(f'Created socket for {self.source_id} with info {str(self.sock.getsockname())} '
                                     f'to peer {self.sock.getpeername()}')
                else:
                    #only for client
                    await asyncio.wait_for(self.loop.sock_connect(self.sock,
                                                (self.proxy['address'], self.proxy['port'])), timeout=self.connect_timeout)                                
                    self.logger.info(f'Created socket for {self.source_id} with info {str(self.sock.getsockname())} '
                                     f'to proxy {self.sock.getpeername()}')
                    await self.proxy_connect(self.server_sockaddr, server_sockaddr_addr=server_sockaddr_addr)
                    
                self.tasks['run_client'] = self.loop.create_task(self.run_client(\
                                              alternate_client_cert_toggle_default=alternate_client_cert_toggle_default))

                if self.keep_alive_period:
                    if not isinstance(self.tasks['run_client'], list):
                        self.tasks['run_client'] = [self.tasks['run_client']]
                    self.tasks['run_client'].append(self.loop.create_task(self.keep_alive_client_check()))
                #self.logger.info('ALL TASKS : '+str(self.tasks['run_client'].all_tasks()))            
              
            try:
                if not os.path.exists(self.active_connectors_path):
                    with open(self.active_connectors_path, 'w') as fd:
                        json.dump([], fd)
                with open(self.active_connectors_path, 'r') as fd:
                    set_active_connectors = json.load(fd)
                if self.is_server:
                    name = ' '.join([str(el) for el in self.server_sockaddr])
                else:
                    name = self.source_id
                set_active_connectors.append(name)
                with open(self.active_connectors_path, 'w') as fd:
                    json.dump(list(set(set_active_connectors)), fd)
            except Exception:
                self.logger.exception('start_connector')                
            return
        except (ConnectionRefusedError, asyncio.TimeoutError) as exc:
            self.logger.warning(f'{str(exc) or type(exc)}')
            if not self.is_server:
                self.tasks['client_wait_for_reconnect'] = self.loop.create_task(self.client_wait_for_reconnect())    
            return                
        except asyncio.CancelledError:
            raise        
        except Exception:
            self.logger.exception('start')
            if not self.is_server:
                self.loop.create_task(self.restart(sleep_between=self.SLEEP_BETWEEN_START_FAILURES))
            else:
                await self.stop(shutdown=True, enable_delete_files=False)
                raise
            return

    async def keep_alive_client_check(self):
        self.logger.info('Starting task keep_alive_client_check')
        server_sockaddr = str(self.server_sockaddr)
        try:
            while server_sockaddr not in self.full_duplex_connections:
                await asyncio.sleep(self.keep_alive_period)
            
            self.logger.info('Starting to send keep_alive_client_check')
            full_duplex = self.full_duplex_connections[server_sockaddr]
        except asyncio.CancelledError:
            raise                            
        except Exception:
            self.logger.exception('keep_alive_client_check')
            raise
            
        number_of_unanswered_keep_alive = 0
        
        while True:
            try:                
                full_duplex.keep_alive_event_received.clear()
                #AWAIT_RESPONSE must be true event if not really used, for consistency in handle_incoming_connection _ping
                transport_json = {MessageFields.AWAIT_RESPONSE:True, MessageFields.REQUEST_ID:self.KEEP_ALIVE_CLIENT_REQUEST_ID,
                                   MessageFields.DESTINATION_ID:server_sockaddr, MessageFields.MESSAGE_TYPE:'_ping'}
                await full_duplex.send_message(transport_json=transport_json, data=self.KEEP_ALIVE_CLIENT_REQUEST_ID)
                try:
                    await asyncio.wait_for(full_duplex.keep_alive_event_received.wait(), timeout=self.keep_alive_timeout)
                    #transport_json, data, binary = await asyncio.wait_for(full_duplex.recv_message(), timeout=self.keep_alive_timeout)
                except asyncio.TimeoutError:
                    self.logger.warning(f'send_message : keep_alive_client_check error ({self.keep_alive_timeout} s)')
                    number_of_unanswered_keep_alive += 1
                else:
                    number_of_unanswered_keep_alive = 0
                finally:
                    full_duplex.keep_alive_event_received.clear()

                if number_of_unanswered_keep_alive >= self.max_number_of_unanswered_keep_alive:
                    self.logger.warning('Restarting client because lack of keep alive responses')
                    #requires immediate socket close
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b'\x01\x00\x00\x00\x00\x00\x00\x00')                                    
                    self.sock.shutdown(socket.SHUT_RDWR)
                    
                    self.tasks['client_wait_for_reconnect'] = self.loop.create_task(self.client_wait_for_reconnect())
                    return
            except asyncio.CancelledError:
                raise                
            except Exception:
                self.logger.exception('keep_alive_client_check')
            await asyncio.sleep(self.keep_alive_period)
        
        
    async def proxy_connect(self, server_sockaddr, server_sockaddr_addr=None):
        #request : CONNECT bla.com:22 HTTP/1.1
        #Proxy-Authorization: Basic <b64encode(username:password)>
        #response : HTTP/1.1 200 OK
        try:
            proxy_msg = f"CONNECT {server_sockaddr_addr or server_sockaddr[0]}:{server_sockaddr[1]} HTTP/1.1"
            authorization = self.proxy.get('authorization', None)
            if authorization:
                if self.hook_proxy_authorization:
                    username, password = self.hook_proxy_authorization(authorization['username'], authorization['password'])
                else:
                    username, password = authorization['username'], authorization['password']
                proxy_msg_cont = "\r\nProxy-Authorization: basic " + \
                b64encode((username+':'+password).encode()).decode()
                proxy_msg += proxy_msg_cont
            proxy_msg += "\r\n\r\n"
            proxy_msg = proxy_msg.encode()
            self.logger.info(f'Trying to connect through proxy with : {proxy_msg}')
            
            if self.proxy.get('ssl_server', None):
                self.logger.info('Proxy server listens with SSL')
                #TODO !!! wrapping the non blocking socket won't work, need to rewrite this
                context = ssl.create_default_context()
                #ssl wrap the client/proxy socket
                self.sock = context.wrap_socket(self.sock)
                #create a sockpair :
                #inside end self.inside_end_sockpair will be used by run_client instead of self.sock
                #outside end should be forwarded to/from self.sock in 2 new tasks appended to run_client task
                self.inside_end_sockpair, self.outside_end_sockpair = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                if not isinstance(self.tasks['run_client'], list):
                    self.tasks['run_client'] = [self.tasks['run_client']]
                self.tasks['run_client'].append(self.loop.create_task(self.internal_proxy_to_socket()))
                self.tasks['run_client'].append(self.loop.create_task(self.socket_to_internal_proxy()))                
                
            await self.loop.sock_sendall(self.sock, proxy_msg)
            resp = await self.loop.sock_recv(self.sock, 2048)
            self.logger.info(f'Proxy server response received : {str(resp)}')
            regex_status = b'HTTP\/\d.\d 200'
            if re.match(regex_status, resp[:12]):
            #if resp.startswith(b'HTTP/1.1 200') or resp.startswith(b'HTTP/1.0 200'):
                self.logger.info('Proxy success')
            else:
                self.logger.error('Proxy failure, raising ConnectionRefusedError')
                raise ConnectionRefusedError()
        except Exception:
            self.logger.exception('proxy_connect')
            raise
        
    async def internal_proxy_to_socket(self):
        #used only in ssl_server
        #forward data received by self.outside_end_sockpair to self.sock
        while True:
            #data sent by client goes from inside_end_sockpair (in run_client) to outside_end_sockpair
            #then we must forward it from self.outside_end_sockpair to self.sock :
            try:
                data = await self.loop.sock_recv(self.outside_end_sockpair, 2048)
                await self.loop.sock_sendall(self.sock, data)
            except Exception:
                self.logger.exception('internal_proxy_to_socket')
            
    async def socket_to_internal_proxy(self):
        #used only in ssl_server        
        #forward data received by self.sock to self.outside_end_sockpair
        while True:
            #we must forward data received by client self.sock into outside_end_sockpair      
            #then this data will reach self.inside_end_sockpair in run_client
            try:
                data = await self.loop.sock_recv(self.sock, 2048)
                await self.loop.sock_sendall(self.outside_end_sockpair, data)
            except Exception:
                self.logger.exception('socket_to_internal_proxy')
            
        
    async def stop(self, connector_socket_only=False, client_wait_for_reconnect=False, hard=False,
                   shutdown=False, enable_delete_files=True):
            
        self.logger.info(f'{self.source_id} Connector stopping ...')    
        self.is_running = False
            
        try:
            #1- stop commander_server
            #2- stop queue_send_to_connector
            #3- stop fullduplex
            #4- cancel_tasks and stop queue_recv_from_connector
            #5- close socket
            #6- delete uds_path_send_to_connector file
            if not connector_socket_only:                
                if self.send_to_connector_server:           
                    try:
                        self.send_to_connector_server.close()   
                        await self.send_to_connector_server.wait_closed()                                         
                    except Exception:
                        self.logger.exception('stop send_to_connector_server')
            
            if self.is_server:
                full_duplex_connections = list(self.full_duplex_connections.values())
                for full_duplex in full_duplex_connections:
                    await full_duplex.stop(hard=hard)
                full_duplex_connections = None
                #self.full_duplex_connections = {}
                try:
                    if self.server:
                        self.server.close()
                        await self.server.wait_closed()
                except Exception:
                    self.logger.exception('stop server')
            else:
                if not client_wait_for_reconnect:
                    full_duplex_connections = list(self.full_duplex_connections.values())                    
                    for full_duplex in full_duplex_connections:
                        await full_duplex.stop(hard=hard)
                    full_duplex_connections = None
                    #self.full_duplex_connections = {}

            task_excludes = ['client_wait_for_reconnect'] if client_wait_for_reconnect else None                    
            if not connector_socket_only:
                #most important here is tasks['queue_recv_from_connector'].cancel()
                self.cancel_tasks(task_excludes=task_excludes)                           
            else:         
                if self.is_server:     
                    self.cancel_tasks(task_names=['run_server'])
                else:
                    self.cancel_tasks(task_names=['run_client'], task_excludes=task_excludes)                 
        except Exception:
            self.logger.exception('cancel tasks')      
        #Close again explicitly just in case        
        try:                
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            #self.logger.exception('shutdown socket')            
            pass
        try:            
            self.sock.close()
        except Exception:
            self.logger.exception('close socket')            
            pass
        
        #######
        #this self.sock=socket.socket(... line is mandatory ! when calling self.restart for client with ssl
        #otherwise the ssl open_connection in run_client hangs forever
        #the reason is unclear
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #######        
        
        if not connector_socket_only and not client_wait_for_reconnect:
            if enable_delete_files:
                try:                
                    if os.path.exists(self.uds_path_send_to_connector):
                        self.logger.info('Deleting file '+self.uds_path_send_to_connector)                    
                        os.remove(self.uds_path_send_to_connector)
                except Exception:
                    self.logger.exception('stop : remove uds_path_send_to_connector')
        
        if shutdown:
            try:
                if self.commander_server:
                    #in case this is a ConnectorManager, not a ConnectorAPI nor a ConnectorRemoteTool
                    self.commander_server.close()
                    await self.commander_server.wait_closed()
                    self.commander_server_task.cancel()
                    if enable_delete_files:
                        if os.path.exists(self.uds_path_commander):                
                            self.logger.info('Deleting file '+self.uds_path_commander)                    
                            os.remove(self.uds_path_commander)
                    self.logger.info('Destroying ConnectorManager '+self.source_id)
            except Exception:
                self.logger.exception('shutdown : remove uds_path_commander')
            #raise Exception('shutdown')
            
        if os.path.exists(self.active_connectors_path):
            try:
                with open(self.active_connectors_path, 'r') as fd:
                    set_active_connectors = json.load(fd)
                if self.is_server:
                    name = ' '.join([str(el) for el in self.server_sockaddr])
                else:
                    name = self.source_id 
                if name in set_active_connectors:
                    set_active_connectors.remove(name)
                    with open(self.active_connectors_path, 'w') as fd:
                        json.dump(set_active_connectors, fd)
            except Exception:
                self.logger.exception('stop_connector')                
            
    async def restart(self, sleep_between=0, connector_socket_only=False, hard=False):    
        if connector_socket_only:
            self.logger.info(f'{self.source_id} Connector restarting socket only ...')            
        else:
            self.logger.info(f'{self.source_id} Connector restarting ...')
        await self.stop(connector_socket_only=connector_socket_only, hard=hard)
        if sleep_between:
            self.logger.warning(f'{self.source_id} restart : Sleeping between {sleep_between} seconds')
            await asyncio.sleep(sleep_between)
            
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        await self.start(connector_socket_only=connector_socket_only)

    async def cancel_tasks_async(self, task_names=None, task_excludes=None):
        self.cancel_tasks(task_names=task_names, task_excludes=task_excludes)
                
    def cancel_tasks(self, task_names=None, task_excludes=None):
        #If task_names=None, cancel all tasks, else cancel task_names list of tasks
        try:
            self.logger.info(f'cancel_tasks called with task_names : {task_names} and task_excludes : {task_excludes}'
                             f' and current self.tasks : {list(self.tasks.keys())}')
            tasks_to_pop = []
            for name, tasks in self.tasks.items():
                
                if (task_names is None) or (name in task_names):
                    if task_excludes and (name in task_excludes):
                        #don't cancel this excluded task
                        continue
                    self.logger.info(f'{self.source_id} Cancelling task : {name}')
                    if isinstance(tasks, list):
                        for the_task in tasks:
                            if the_task:
                                self.logger.info(f'{self.source_id} Cancelling task in {name} : {the_task}')
                                the_task.cancel()
                    else:
                        if tasks:
                            tasks.cancel()
  
                    tasks_to_pop.append(name)

            for name in tasks_to_pop:
                self.tasks.pop(name)
        except Exception:
             self.logger.exception(self.source_id+' cancel_tasks')
                     
    async def client_wait_for_reconnect(self):
        #queue_send_to_connector_put should continue working as usual during disconnect mode !
        #we don't want to lose events at transition
        self.logger.info(f'{self.source_id} client_wait_for_reconnect client entering Disconnected mode')      
        await self.stop(connector_socket_only=True, client_wait_for_reconnect=True)
        
        if not self.enable_client_try_reconnect:
            self.logger.info(f'{self.source_id} client_wait_for_reconnect leaving because of enable_client_try_reconnect')      
            return
        
        count = 1               
        while True:
            self.logger.info(f'{self.source_id} Client will try to reconnect in {self.SLEEP_BETWEEN_START_FAILURES} seconds')
            await asyncio.sleep(self.SLEEP_BETWEEN_START_FAILURES)
            
            #try to tcp connect to test connectivity
            #in ssl mode, this connection will not reach our connector since it won't go through the ssl handhake
            #but in nossl mode, this connection will be seen as a real connector connection by peer, and spawn a FullDUplex
            #instance, which will receive the disconnection (sock.close) in recv_message.
            #Then the real peer connection will immediately follow.
            #This is not perfect, and is seen in the logs, but good enough.
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)        
                self.sock.setblocking(False)                
                if self.client_bind_ip:
                    sock_bind = self.client_bind_ip
                    if not '.' in sock_bind:
                        self.logger.info(f'Trying to bind {self.client_bind_ip} with a correct ip')
                        sock_bind = iface_to_ip(sock_bind)
                        self.logger.info(f'Binding {self.client_bind_ip} using {sock_bind}')                         
                    self.sock.bind((sock_bind,0))
                server_sockaddr_addr = self.server_sockaddr[0]
                if '.' not in server_sockaddr_addr:
                    #this is useful only when client and server are on the same machine                    
                    server_sockaddr_addr = iface_to_ip(server_sockaddr_addr)

                if not self.proxy.get('enabled'):
                    await asyncio.wait_for(self.loop.sock_connect(self.sock,
                                                (server_sockaddr_addr, self.server_sockaddr[1])), timeout=self.connect_timeout)                                
                    self.logger.debug(f'Created socket for {self.source_id} with info {str(self.sock.getsockname())} '
                                     f'to peer {self.sock.getpeername()}')
                else:
                    #only for client
                    await asyncio.wait_for(self.loop.sock_connect(self.sock,
                                                (self.proxy['address'], self.proxy['port'])), timeout=self.connect_timeout)                                
                    self.logger.debug(f'Created socket for {self.source_id} with info {str(self.sock.getsockname())} '
                                     f'to proxy {self.sock.getpeername()}')
                    await self.proxy_connect(self.server_sockaddr, server_sockaddr_addr=server_sockaddr_addr)
                      
            except asyncio.CancelledError:
                raise
            except Exception as exc:    #(ConnectionRefusedError, asyncio.TimeoutError):
                self.logger.error(f'{self.source_id} client_wait_for_reconnect failed connection attempt number {count} '
                                  f'because {str(exc) or type(exc)}')
                count += 1
                continue
            finally:
                try:
                    self.sock.shutdown()                    
                    self.sock.close()
                except Exception:
                    pass
            
            self.logger.info(f'{self.source_id} client_wait_for_reconnect connectivity was reestablished !')
            
            if self.alternate_client_default_cert:    #this comes from configuration, changed only at restart
                client_reconnect_now_timestamp = time()
                delta = client_reconnect_now_timestamp - self.client_reconnect_last_timestamp
                self.client_reconnect_last_timestamp = client_reconnect_now_timestamp
                #if failures every 5s, 1st is private, 2nd is default, 3rd private, 4th default, etc
                if delta < (self.SLEEP_BETWEEN_START_FAILURES + 1):
                    self.alternate_client_cert_toggle_default = not self.alternate_client_cert_toggle_default
                else:
                    self.alternate_client_cert_toggle_default = False
                
            await self.start(connector_socket_only=True, alternate_client_cert_toggle_default=self.alternate_client_cert_toggle_default)
                       
            self.logger.info(f'{self.source_id} client_wait_for_reconnect client entering back Connected mode')                        
            return
        
    async def commander_cb(self, reader, writer):
        try:
            next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
            next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
            payload = await asyncio.wait_for(reader.readexactly(next_length), timeout=Connector.ASYNC_TIMEOUT)
            payload_json = json.loads(payload)
            cmd = payload_json.get('cmd')
            kwargs = payload_json.get('kwargs')
            if cmd:
                cmd_mode = 'async'
                if '__sync' in cmd:
                    cmd = cmd[:-len('__sync')]
                    cmd_mode = 'sync'
                if not hasattr(self, cmd):
                    self.logger.warning('Unknown command : '+str(cmd))
                    return
                self.logger.info(f'commander_cb calling {cmd} with arguments {kwargs}')
                if cmd_mode == 'async':
                    response = await getattr(self, cmd)(**kwargs)
                else:
                    response = getattr(self, cmd)(**kwargs)
                if response:
                    response = str(response).encode()
                else:
                    response = b'{}' #b'success'    
                response = Structures.MSG_4_STRUCT.pack(len(response)) + response
                self.logger.info(f'commander_cb responding : {response}')
                writer.write(response)            
                try:
                    await asyncio.wait_for(writer.drain(), timeout=Connector.ASYNC_TIMEOUT)
                except Exception:
                    self.logger.exception('commander_cb writer drain')     
            writer.close()
            if PYTHON_GREATER_37:
                try:
                    await writer.wait_closed()      #python 3.7                                              
                except Exception as exc:
                    self.logger.warning('commander_cb writer.wait_closed : '+str(exc))
            
        except asyncio.CancelledError:
            raise            
        except Exception:
            self.logger.exception('commander_cb')

    def peek_queues(self, dump_result=False):
        res = {'queue_send':{}, 'queue_recv':None}
        if self.queue_recv:
            res['queue_recv'] = self.queue_recv.qsize()
        for peer, queue in self.queue_send.items():
            res['queue_send'][peer] = queue.qsize()
        if dump_result:
            return json.dumps(res, indent=4, sort_keys=True)
        return res

    def show_connected_peers(self, dump_result=True):
        #res = list(sorted(self.full_duplex_connections.keys()))
        
        res = {thekey:list(self.full_duplex_connections[thekey].extra_info) for thekey in \
               list(sorted(self.full_duplex_connections.keys()))}
        
        #if self.is_server:
        #    res = list(sorted(self.full_duplex_connections.keys()))
        #elif self.queue_send:
        #    res = list(sorted(self.queue_send.keys()))            
        if dump_result:
            return json.dumps(res)
        return res

    def manage_ignore_peer_traffic(self, show=False, enable=False, disable=False, unique_peer=False):
        if show:
            return str(self.ignore_peer_traffic)
        if enable:
            self.ignore_peer_traffic = True
            kwargs = {'ignore_peer_traffic': True}
            self.update_config_file(kwargs)
            return str(self.ignore_peer_traffic)
        if disable:
            self.ignore_peer_traffic = False
            kwargs = {'ignore_peer_traffic': False}
            self.update_config_file(kwargs)            
            return str(self.ignore_peer_traffic)
        if unique_peer:
            self.ignore_peer_traffic = [unique_peer]
            kwargs = {'ignore_peer_traffic': unique_peer}
            self.update_config_file(kwargs)            
            return str(self.ignore_peer_traffic)

    async def disconnect_client(self, client_id):
        if not self.is_server:
            msg = f'{self.source_id} client cannot disconnect a client {client_id}'
            self.logger.warning(msg)
            return False
        self.logger.info(f'{self.source_id} disconnecting client {client_id}')
        full_duplex = self.full_duplex_connections.pop(client_id, None)
        if not full_duplex:
            self.logger.info(f'{self.source_id} cannot disconnect non existing client {client_id}')
            return f'Non existing client {client_id}'
        await full_duplex.stop()
        return True     

    async def add_blacklist_client(self, client_ip=None, client_id=None):
        #disconnect and add to blacklisted_ list
        if not self.is_server:
            msg = f'{self.source_id} client cannot blacklist a client {client_id} {client_ip}'
            self.logger.warning(msg)
            return False
        try:
            kwargs = {}
            if client_id:
                self.logger.info(f'{self.source_id} blacklisting client {client_id}')            
                self.blacklisted_clients_id.add(client_id)
                kwargs['blacklisted_clients_id'] = list(self.blacklisted_clients_id)
                #full_duplex = self.full_duplex_connections.pop(client_id, None)            
                #client_ip = full_duplex.extra_info[0]   
                #full_duplex = None
                if self.use_token:
                    await self.delete_client_token_on_server(client_id=client_id)
                else:
                    await self.delete_client_certificate_on_server(client_id=client_id)
            elif client_ip:
                self.logger.info(f'{self.source_id} blacklisting client {client_ip}')        
                if '/' in client_ip:
                    self.blacklisted_clients_subnet.add(ipaddress.IPv4Network(client_ip))
                    kwargs['blacklisted_clients_subnet'] = [str(el) for el in self.blacklisted_clients_subnet]
                else:
                    self.blacklisted_clients_ip.add(client_ip)
                    kwargs['blacklisted_clients_ip'] = list(self.blacklisted_clients_ip)
                #cannot disconnect client by ip
                #await self.disconnect_client(client_id?)                    
            self.update_config_file(kwargs)
            return True     
        except Exception:
            msg = f'{self.source_id} client could not blacklist a client {client_id} {client_ip}'
            self.logger.exception(msg)
            return False

    async def remove_blacklist_client(self, client_ip=None, client_id=None):
        if not self.is_server:
            msg = f'{self.source_id} client cannot remove blacklisted client {client_id} {client_ip}'
            self.logger.warning(msg)
            return False
        try:
            kwargs = {}
            if client_id:
                self.logger.info(f'{self.source_id} removing blacklisted client {client_id}')            
                self.blacklisted_clients_id.remove(client_id)
                kwargs['blacklisted_clients_id'] = list(self.blacklisted_clients_id)                
            elif client_ip:
                self.logger.info(f'{self.source_id} removing blacklisted client {client_ip}')        
                if '/' in client_ip:
                    self.blacklisted_clients_subnet.remove(ipaddress.IPv4Network(client_ip))
                    kwargs['blacklisted_clients_subnet'] = [str(el) for el in self.blacklisted_clients_subnet]                                    
                else:
                    self.blacklisted_clients_ip.remove(client_ip)            
                    kwargs['blacklisted_clients_ip'] = list(self.blacklisted_clients_ip)                
            self.update_config_file(kwargs)                    
            return True     
        except Exception:
            msg = f'{self.source_id} client could not remove blacklisted client {client_id} {client_ip}'
            self.logger.exception(msg)
            return False


    async def add_whitelist_client(self, client_ip=None, client_id=None):
        if not self.is_server:
            msg = f'{self.source_id} client cannot whitelist a client {client_id} {client_ip}'
            self.logger.warning(msg)
            return False
        try:
            kwargs = {}
            if client_id:
                self.logger.info(f'{self.source_id} whitelisting client {client_id}')            
                self.whitelisted_clients_id.add(client_id)
                kwargs['whitelisted_clients_id'] = list(self.whitelisted_clients_id)
            elif client_ip:
                self.logger.info(f'{self.source_id} whitelisting client {client_ip}')        
                if '/' in client_ip:
                    self.whitelisted_clients_subnet.add(ipaddress.IPv4Network(client_ip))
                    kwargs['whitelisted_clients_subnet'] = [str(el) for el in self.whitelisted_clients_subnet]                    
                else:
                    self.whitelisted_clients_ip.add(client_ip)
                    kwargs['whitelisted_clients_ip'] = list(self.whitelisted_clients_ip)   
            self.update_config_file(kwargs)                                     
            return True     
        except Exception:
            msg = f'{self.source_id} client could not whitelist a client {client_id} {client_ip}'
            self.logger.exception(msg)
            return False

    async def remove_whitelist_client(self, client_ip=None, client_id=None):
        if not self.is_server:
            msg = f'{self.source_id} client cannot remove whitelisted client {client_id} {client_ip}'
            self.logger.warning(msg)
            return False
        try:
            kwargs = {}
            if client_id:
                self.logger.info(f'{self.source_id} removing whitelisted client {client_id}')            
                self.whitelisted_clients_id.remove(client_id)
                kwargs['whitelisted_clients_id'] = list(self.whitelisted_clients_id)                
            elif client_ip:
                self.logger.info(f'{self.source_id} removing whitelisted client {client_ip}')        
                if '/' in client_ip:
                    self.whitelisted_clients_subnet.remove(ipaddress.IPv4Network(client_ip))
                    kwargs['whitelisted_clients_subnet'] = [str(el) for el in self.whitelisted_clients_subnet]
                else:
                    self.whitelisted_clients_ip.remove(client_ip)
                    kwargs['whitelisted_clients_ip'] = list(self.whitelisted_clients_ip)  
            self.update_config_file(kwargs)                                                         
            return True     
        except Exception:
            msg = f'{self.source_id} client could not remove whitelisted client {client_id} {client_ip}'
            self.logger.exception(msg)
            return False
    
    async def delete_client_certificate_on_server(self, client_id=None, remove_only_symlink=False):
        try:
            self.logger.info(f'{self.source_id} deleting client certificate {client_id}')
            if self.use_ssl:
                response = await self.ssl_helper.remove_client_cert_on_server(client_id, remove_only_symlink=remove_only_symlink)
            else:
                response = True
            await self.disconnect_client(client_id)
            return response
        except Exception as exc:
            self.logger.exception('delete_client_certificate_on_server')
            return json.dumps({'status':False, 'msg':str(exc)})

    async def delete_client_token_on_server(self, client_id=None):
        try:
            self.logger.info(f'{self.source_id} deleting client token {client_id}')
            tokens_dict = self.load_server_tokens()
            if client_id in tokens_dict:
                tokens_dict.pop(client_id)
                self.store_server_tokens(tokens_dict)
            await self.disconnect_client(client_id)
            return json.dumps({'status':True, 'msg':''})
        except Exception as exc:
            self.logger.exception('delete_client_token_on_server')
            return json.dumps({'status':False, 'msg':str(exc)})

    async def delete_client_certificate_on_client(self, restart_client=True):
        try:
            self.logger.info(f'{self.source_id} deleting own certificate')            
            response = await self.ssl_helper.remove_client_cert_on_client(self.source_id)
            self.client_certificate_name = self.ssl_helper.CLIENT_DEFAULT_CERT_NAME
            self.logger.info('Client will use again the default certificate : '+self.client_certificate_name)
            if restart_client:
                await self.restart()            
            return response
        except Exception as exc:
            self.logger.exception('delete_client_certificate_on_client')
            return json.dumps({'status':False, 'msg':str(exc)})

    async def delete_client_token_on_client(self, restart_client=True):
        try:
            self.logger.info(f'{self.source_id} deleting own token')
            if os.path.exists(self.tokens_file_path):
                os.remove(self.tokens_file_path)
            self.token = None
            if restart_client:
                await self.restart()            
            return json.dumps({'status':True, 'msg':''})
        except Exception as exc:
            self.logger.exception('delete_client_token_on_client')
            return json.dumps({'status':False, 'msg':str(exc)})

    def show_log_level(self):
        res = logging.getLevelName(self.logger.getEffectiveLevel())
        return res

    def set_log_level(self, level):
        self.logger.setLevel(level)
        return level

    def show_attribute(self, attribute):
        res = str(getattr(self, attribute))
        return res
    
    def delete_previous_persistence_remains(self):
        try:
            self.logger.info(f'{self.source_id} delete_previous_persistence_remains checking files')
            
            persistence_dir = os.path.dirname(self.persistence_path)
            persistence_basename = os.path.basename(self.persistence_path)
            for filename in os.listdir(persistence_dir):            
                if filename.startswith(persistence_basename):
                    old_persistence_path = os.path.join(persistence_dir, filename)
                    self.logger.warning(f'{self.source_id} delete_previous_persistence_remains '
                                        f'Deleting old persistent file : {old_persistence_path}')
                    os.remove(old_persistence_path)
                    
            persistence_recv_dir = os.path.dirname(self.persistence_recv_path)
            persistence_recv_basename = os.path.basename(self.persistence_recv_path)
            for filename in os.listdir(persistence_recv_dir):            
                if filename.startswith(persistence_recv_basename):
                    old_persistence_path = os.path.join(persistence_recv_dir, filename)
                    self.logger.warning(f'{self.source_id} delete_previous_persistence_remains '
                                        f'Deleting old persistent file : {old_persistence_path}')
                    os.remove(old_persistence_path)
                    
        except Exception:
            self.logger.exception(f'{self.source_id} delete_previous_persistence_remains')
                
    def show_subscribe_message_types(self):
        self.logger.info(f'{self.source_id} show_subscribe_message_types')
        if not self.is_server:
            return self.subscribe_message_types
        else:
            return []

    async def set_subscribe_message_types(self, message_types):
        self.logger.info(f'{self.source_id} set_subscribe_message_types {message_types}')
        message_types_to_remove = [message_type for message_type in message_types \
                                   if message_type not in self.recv_message_types]
        if message_types_to_remove:
            self.logger.warning(f'Ignoring {message_types_to_remove} because they are not in recv_message_types')
        message_types = list(set(message_types) - set(message_types_to_remove))
        if not self.is_server:
            try:
                unsubscribe_to_message_types = set(self.subscribe_message_types) - set(message_types)
                subscribe_to_message_types = set(message_types) - set(self.subscribe_message_types)
                send_message = self.full_duplex_connections[str(self.server_sockaddr)].send_message
                if unsubscribe_to_message_types:
                    await send_message(message_type='_pubsub',
                                data=json.dumps({'unsubscribe_message_types':list(unsubscribe_to_message_types)}))
                if subscribe_to_message_types:
                    await send_message(message_type='_pubsub',
                                data=json.dumps({'subscribe_message_types':list(subscribe_to_message_types)}))                
                self.subscribe_message_types = message_types
            except Exception:
                self.logger.exception('set_subscribe_message_types')
            return self.subscribe_message_types
        else:
            return []
            
    def store_message_to_persistence(self, peername, message, ignore_count=False):
        persistence_path = self.persistence_path + self.alnum_name(peername)
        try:
            new_size = len(message) + len(self.PERSISTENCE_SEPARATOR)
            if os.path.exists(persistence_path):
                new_size += os.path.getsize(persistence_path)
            if new_size > self.max_size_persistence_path:
                self.logger.warning(f'{self.source_id} Cannot store message of size {len(message)} to persistence '
                                    f'for peer {peername} because {persistence_path} is too big')                
                return
            self.logger.debug(f'{self.source_id} Storing message to persistence to peer {peername}')
            if DEBUG_SHOW_DATA:
                self.logger.info('With data : '+str(message[200:210]))
            if self.PERSISTENCE_SEPARATOR in message:
                self.logger.warning(f'{self.source_id} message to persistence to peer {peername} must be altered')
                message = message.replace(self.PERSISTENCE_SEPARATOR, self.PERSISTENCE_SEPARATOR_REPLACEMENT)
            with open(persistence_path, mode='ab') as fd:
                fd.write(self.PERSISTENCE_SEPARATOR)
                #fd.write(peername.encode()+b'\n')
                fd.write(message)
            if self.debug_msg_counts and not ignore_count:
                #ignore_count is nice to understand we are in a Special case. But msg_counts['load_persistence'] will still contain duplicates
                self.msg_counts['store_persistence_send'] += 1
                
        except Exception:
            self.logger.exception(f'{self.source_id} store_message_to_persistence')        

    def persistence_existence_check(self, peername):
        persistence_path = self.persistence_path + self.alnum_name(peername)
        if not os.path.exists(persistence_path):
            self.logger.info(f'{self.source_id} has no available persistence data for peer {peername}')            
            return False
        self.logger.info(f'{self.source_id} has available persistence data at {persistence_path} for peer {peername}')
        return True            
            
    async def load_messages_from_persistence(self, peername):
        self.logger.info(f'{self.source_id} Loading messages from persistence')
        queue_send = self.queue_send_transition_to_connect.get(peername)
        if queue_send is None:
            self.logger.warning(f'{self.source_id} Cannot load persistence message to absent queue {peername}')
            return            
       # async with self.persistence_lock:                       
        persistence_path = self.persistence_path + self.alnum_name(peername)
        try:
            #if not os.path.exists(persistence_path):
            #    self.logger.info('The persistence path '+persistence_path+' does not exist')
            #    return
            persistent_count = 0
            fd = open(persistence_path, mode='rb')
            try:
                last_element = b''
                last_iteration = False                
                if fd.read(len(self.PERSISTENCE_SEPARATOR)) != self.PERSISTENCE_SEPARATOR:
                    self.logger.warning(f'Invalid persistence file {persistence_path}')
                else:
                    while True:
                        #read file in chunks
                        chunk = fd.read(self.READ_PERSISTENCE_CHUNK_SIZE)
                        if not chunk:
                            last_iteration = True                    
                        chunk = last_element + chunk
                        #split chunks in message units to put to queue
                        persistence_units = chunk.split(self.PERSISTENCE_SEPARATOR)
                        if not last_iteration:
                            #last_element may be a partial separator to be completed in next read chunk                        
                            last_element = persistence_units[-1]
                            persistence_units = persistence_units[:-1]
                        for message in persistence_units:
                            self.logger.debug('Loading persistent message to queue : '+peername)                 
                            message_tuple = self.unpack_message(message)
                            if DEBUG_SHOW_DATA:
                                self.logger.debug('With data : '+str(message_tuple[1][:10]))
                            if queue_send.full():
                                self.logger.info(f'{self.source_id} : No room in queue_send for more messages'
                                                 f' after persistent_count {persistent_count} messages')
                                last_iteration = True
                                break
                            if self.SUPPORT_PRIORITIES:
                                #insert 2 priority null fields, expected by queue_send get
                                await queue_send.put((None, None, message_tuple))
                            else:
                                await queue_send.put(message_tuple)
                            persistent_count += 1
                            #sleep(0) is important otherwise queue_send_to_connector_put may have losses under high loads
                            #because of this cpu intensive loop
                            await asyncio.sleep(0)#0.001)
                            if self.debug_msg_counts:
                                self.msg_counts['load_persistence_send']+=1                            
    
                        if last_iteration:
                            break
            except Exception:
                self.logger.exception('open persistence')
            finally:
                fd.close()
            self.logger.info(f'{self.source_id} load_messages_from_persistence finished loading {persistent_count} '
                             f'messages to queue_send_transition_to_connect. Deleting persistence file {persistence_path}')
            try:
                os.remove(persistence_path)
            except Exception:
                self.logger.exception(f'{self.source_id} load_messages_from_persistence delete persistence file')
        except asyncio.CancelledError:
            raise                            
        except Exception:
            self.logger.exception(f'{self.source_id} load_messages_from_persistence')        
        
        
    #4|2|json|4|data|4|binary
    def pack_message(self, transport_json=None, message_type=None, source_id=None, destination_id=None,
                     request_id=None, response_id=None, binary=None, await_response=False,
                     with_file=None, data=None, wait_for_ack=False, message_type_publish=None):
        if DEBUG_SHOW_DATA:
            self.logger.debug('pack_message with params : '+str(message_type)+', '+str(data)+', '+str(transport_json))
        if transport_json is None:
            transport_json = {MessageFields.MESSAGE_TYPE : message_type or self.send_message_types[0]}
            if source_id is not None:
                transport_json[MessageFields.SOURCE_ID] = source_id
            if destination_id is not None:
                transport_json[MessageFields.DESTINATION_ID] = destination_id           
            if request_id is not None:
                transport_json[MessageFields.REQUEST_ID] = request_id
            if response_id is not None:
                transport_json[MessageFields.RESPONSE_ID] = response_id            
            if binary:
                transport_json[MessageFields.WITH_BINARY] = True
            if await_response:
                transport_json[MessageFields.AWAIT_RESPONSE] = True    
            if with_file:
                transport_json[MessageFields.WITH_FILE] = with_file
            if wait_for_ack:
                transport_json[MessageFields.WAIT_FOR_ACK] = wait_for_ack
            if message_type_publish:
                transport_json[MessageFields.MESSAGE_TYPE_PUBLISH] = message_type_publish
                
        #pack message
        json_field = json.dumps(transport_json).encode()
        if isinstance(data, str):
            data = data.encode()
        if data is None:
            data = b''
        message = Structures.MSG_2_STRUCT.pack(len(json_field)) + json_field + Structures.MSG_4_STRUCT.pack(len(data)) + data
        if binary:
            message += (Structures.MSG_4_STRUCT.pack(len(binary)) + binary)
        message = Structures.MSG_4_STRUCT.pack(len(message)) + message
        return message
        
    #4|2|json|4|data|4|binary
    def unpack_message(self, message):
        #receives full message in bytes
        #next_length_4 = Structures.MSG_4_STRUCT.unpack(message[:Structures.MSG_4_STRUCT.size])[0]
        next_pointer = Structures.MSG_4_STRUCT.size+Structures.MSG_2_STRUCT.size                
        next_length_2 = Structures.MSG_2_STRUCT.unpack(message[Structures.MSG_4_STRUCT.size:next_pointer])[0]      
        transport_json = json.loads(message[next_pointer:next_pointer+next_length_2])
        next_pointer += next_length_2
        length_data = Structures.MSG_4_STRUCT.unpack(message[next_pointer:next_pointer+Structures.MSG_4_STRUCT.size])[0]
        next_pointer += Structures.MSG_4_STRUCT.size                
        data = message[next_pointer:next_pointer+length_data]
        binary = None
        if transport_json.get(MessageFields.WITH_BINARY):
            next_pointer += length_data                                    
            length_binary = Structures.MSG_4_STRUCT.unpack(message[next_pointer:next_pointer+Structures.MSG_4_STRUCT.size])[0]
            next_pointer += Structures.MSG_4_STRUCT.size                                    
            binary = message[next_pointer:next_pointer+length_binary]
        return transport_json, data, binary    #json, bytes, bytes

    '''
    def extract_transport_json(self, message, message_includes_packet_length=True):
        if message_includes_packet_length:
            transport_json_length_start_index = Structures.MSG_4_STRUCT.size
            transport_json_start_index = transport_json_length_start_index + Structures.MSG_2_STRUCT.size
        else:
            transport_json_length_start_index = 0
            transport_json_start_index = transport_json_length_start_index + Structures.MSG_2_STRUCT.size
        transport_field = message[transport_json_start_index : (transport_json_start_index+Structures.MSG_2_STRUCT.unpack(message[transport_json_length_start_index:transport_json_length_start_index+Structures.MSG_2_STRUCT.size])[0])]            
        return json.loads(transport_field)
    
    def recalculate_message_transport_json(self, payload, updated_transport_json):
        remaining_message = payload[Structures.MSG_2_STRUCT.size + len(updated_transport_json):]
        json_field = json.dumps(updated_transport_json).encode()            
        new_message_header = Structures.MSG_2_STRUCT.pack(len(json_field)) + json_field
        new_payload = new_message_header + remaining_message
        message = Structures.MSG_4_STRUCT.pack(len(new_payload)) + new_payload
        return message
    '''
    
    async def queue_send_to_connector(self):
        self.logger.info(f'{self.source_id} queue_send_to_connector task created')
        if os.path.exists(self.uds_path_send_to_connector) and not self.reuse_uds_path_send_to_connector:
            raise Exception(f'{self.uds_path_send_to_connector} already exists. Cannot queue_send_to_connector')

        server = self.send_to_connector_server = await asyncio.start_unix_server(self.queue_send_to_connector_put, 
                                                path=self.uds_path_send_to_connector, limit=self.MAX_SOCKET_BUFFER_SIZE)
        if self.everybody_can_send_messages:
            chown_nobody_permissions(self.uds_path_send_to_connector, self.logger)
        else:
            try:
                os.chmod(self.uds_path_send_to_connector, stat.S_IRWXU | stat.S_IRWXG)
            except Exception:
                self.logger.exception(f'{self.source_id} could not set permissions for {self.uds_path_send_to_connector}')
        return server

    async def queue_send_to_connector_put(self, reader, writer):
        # receives from uds socket, writes to queue_send
        #4|2|json|4|data|4|binary    
        try:
            while True:
                if self.debug_msg_counts:
                    self.msg_counts['msg_recvd_uds'] += 1            
                self.logger.debug(f'{self.source_id} Expecting data from unix clients to queue_send_to_connector')
                next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
                next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
                #payload = 2|json|4|data|4|binary
                payload = await asyncio.wait_for(reader.readexactly(next_length), timeout=self.ASYNC_TIMEOUT)
                message = next_length_bytes + payload
                message_tuple = transport_json , data, binary = self.unpack_message(message)
                self.logger.debug(f'{self.source_id} queue_send_to_connector_put Received message with : '
                                  f'{transport_json}')                
                peername = transport_json.get(MessageFields.DESTINATION_ID)
                if not self.is_server:
                    if not peername:
                        transport_json[MessageFields.DESTINATION_ID] = peername = str(self.server_sockaddr)
                    elif peername != str(self.server_sockaddr):
                        self.logger.warning(f'{self.source_id} queue_send_to_connector_put : overriding invalid '
                                            f'destination id {peername} instead of {self.server_sockaddr}')
                        transport_json[MessageFields.DESTINATION_ID] = peername = str(self.server_sockaddr)
                
                if self.ignore_peer_traffic:
                    ignore_traffic = False
                    if self.ignore_peer_traffic is True:
                        ignore_traffic = True
                    elif peername in self.ignore_peer_traffic:
                        ignore_traffic = True
                    if ignore_traffic:
                        self.logger.debug(f'{self.source_id} queue_send_to_connector_put : Ignoring message to peer {peername}')
                        if not self.uds_path_send_preserve_socket:
                            writer.close()
                            if PYTHON_GREATER_37:
                                try:
                                    await writer.wait_closed()      #python 3.7                                              
                                except Exception as exc:
                                    self.logger.warning('queue_send_to_connector_put1 writer.wait_closed : '+str(exc))                            
                        await asyncio.sleep(0)
                        if not self.uds_path_send_preserve_socket:
                            return
                        continue
                        
                #sanity check, that source_id exists and is valid : that way api doesn't have to ensure source_id, and cannot modify it
                #but still it is more efficient to rely on api source_id
                if transport_json.get(MessageFields.SOURCE_ID) != self.source_id:
                    self.logger.warning(f'{self.source_id} queue_send_to_connector_put : overriding invalid source id '
                                        f'{transport_json.get(MessageFields.SOURCE_ID)} instead of {self.source_id}')
                    #update source_id in transport_json
                    transport_json[MessageFields.SOURCE_ID] = self.source_id
                
                await_response = False
                send_to_queue = True
                
                #if self.queue_send doesn't have a peername key, use persistence if enabled
                if peername not in self.queue_send:
                    disk_persistence = self.disk_persistence
                    if isinstance(disk_persistence, list):
                        #disk_persistence can be a list of message types
                        disk_persistence = (transport_json.get(MessageFields.MESSAGE_TYPE) in disk_persistence)
                    if disk_persistence:
                        ##async with self.persistence_lock:     
                            ##check again the mode, since it may have changed during load_messages_from_persistence
                        self.logger.debug(f'{self.source_id} queue_send_to_connector_put storing message to '
                                          f'persistence for queue {peername}')
                        send_to_queue = False                                
                        self.store_message_to_persistence(peername=peername, message=message)      
                    elif not self.ram_persistence:
                        #self.logger.info('Not adding message to queue_send_to_connector_put because ram_persistence is false')                        
                        self.logger.debug(f'{self.source_id} Not forwarding message to absent peer {peername} to '
                                          'queue_send_to_connector_put because no persistence')                    
                        send_to_queue = False
                            
                message_type = transport_json[MessageFields.MESSAGE_TYPE]
                if (message_type not in self.send_message_types) and (message_type != '_ping') \
                            and (message_type != '_pubsub') and (not self.pubsub_central_broker):
                    self.logger.warning(f'{self.source_id} queue_send_to_connector_put received a message with '
                                        f'invalid type {message_type}. Ignoring...')                    
                    send_to_queue = False                    
                elif transport_json.get(MessageFields.AWAIT_RESPONSE):
                    request_id = transport_json.get(MessageFields.REQUEST_ID)
                    if request_id is None:
                        self.logger.warning(f'{self.source_id} sending message with AWAIT_RESPONSE enabled, but '
                                            'without request_id')
                        #if no application level request_id has been set, we create a dummy unique request_id to be able to detect response
                        #in case peer's application responds smartly, otherwise AWAIT_RESPONSE_TIMEOUT will stop the waiting
                        request_id = uuid.uuid4().hex
                        transport_json[MessageFields.REQUEST_ID] = request_id
                    if request_id in self.messages_awaiting_response[message_type].get(peername, {}):
                        self.logger.warning(f'Request id {request_id} for type {message_type} for peer {peername} '
                                            'already in self.messages_awaiting_response, overriding it !')                       
#                        self.logger.warning(f'Request id {request_id} for type {message_type} for peer {peername} '
#                                            'already in self.messages_awaiting_response, ignoring')
#                        send_to_queue = False
#                    else:
                    if not send_to_queue:
                        if self.disk_persistence or self.ram_persistence:
                            self.logger.warning(f'{self.source_id} Connector is currently disconnected. '
                                                f'Response to request_id {request_id} will be sent when peer goes up'
                                                ', since persistence is enabled')
                            await_response = True                        
                        else:
                            self.logger.warning(f'{self.source_id} Connector is currently disconnected. '
                                                f'Response to request_id {request_id} will not be sent since '
                                                'persistence is disabled')
                            #close writer so that client stops waiting uselessly
                            writer.close()
                            if PYTHON_GREATER_37:
                                try:
                                    await writer.wait_closed()      #python 3.7                                              
                                except Exception as exc:
                                    self.logger.warning('queue_send_to_connector_put2 writer.wait_closed : '+str(exc))
                            
                    else:
                        if len(self.messages_awaiting_response[message_type].get(peername, {})) > self.MAX_NUMBER_OF_AWAITING_REQUESTS:
                            self.logger.error(f'{self.source_id} messages_awaiting_response dict has '
                                              f'{len(self.messages_awaiting_response[message_type].get(peername,{}))}'
                                              f' entries for message type {message_type} and peername {peername}. '
                                              'Stop adding entries until it goes '
                                              f'below {self.MAX_NUMBER_OF_AWAITING_REQUESTS}')
                        else:
                            self.logger.info(f'{self.source_id} Adding request {request_id} to '
                                             f'messages_awaiting_response dict for type {message_type} and '
                                             f'peer {peername}')
                            await_response = True
                    if await_response:
                        if peername not in self.messages_awaiting_response[message_type]:
                            self.messages_awaiting_response[message_type][peername] = {}
                        #2nd element will contain the response set by handle_incoming_connection
                        self.messages_awaiting_response[message_type][peername][request_id] = [asyncio.Event(),None]    
                            
                if send_to_queue:            
                    queue_send = self.queue_send[peername]
                    self.logger.debug(f'{self.source_id} Putting message from queue_send_to_connector_put to queue of {peername}')
                    try:
                        if self.debug_msg_counts:
                            self.msg_counts['send_no_persist']+=1
                        if self.SUPPORT_PRIORITIES:
                            queue_send.put_nowait((self.send_message_types_priorities.get(message_type, 0), time(), message_tuple))
                        else:
                            queue_send.put_nowait(message_tuple)
                    except Exception:
                        self.logger.exception('queue_send.put_nowait')
                        
                if await_response:
                    try:
                        await asyncio.wait_for(self.messages_awaiting_response[message_type][peername][request_id][0].wait(), 
                                               timeout=self.AWAIT_RESPONSE_TIMEOUT)
                    except asyncio.TimeoutError:
                        self.logger.warning(f'{self.source_id} Request id {request_id} timed out awaiting response')
                    else:
                        transport_json, data, binary = self.messages_awaiting_response[message_type][peername][request_id][1]
                        self.logger.info(f'{self.source_id} Receiving response and removing request {request_id} '
                                         f'from messages_awaiting_response dict for type {message_type} '
                                         f'and peer {peername}')
                        message_bytes = self.pack_message(transport_json=transport_json, data=data, binary=binary)
                        # send the length to be sent next                    
                        writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])
                        writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
                        await writer.drain()
                        #await asyncio.wait_for(writer.drain(), timeout=self.ASYNC_TIMEOUT)                                       
                        #response = self.UDS_TERMINATOR
                        #writer.write(response)                   
                    del self.messages_awaiting_response[message_type][peername][request_id]
                    if len(self.messages_awaiting_response[message_type][peername]) == 0:
                        del self.messages_awaiting_response[message_type][peername]
                    
                #response = self.UDS_TERMINATOR
                #writer.write(response)
                #await writer.drain()
                if not self.uds_path_send_preserve_socket:
                    writer.close()
                    if PYTHON_GREATER_37:
                        try:
                            await writer.wait_closed()      #python 3.7                                              
                        except Exception as exc:
                            self.logger.warning('queue_send_to_connector_put3 writer.wait_closed : '+str(exc))
                    
                await asyncio.sleep(0)
                #self.logger.debug(f'{self.source_id} Finished queue_send_to_connector_put task.')
                if not self.uds_path_send_preserve_socket:
                    return
        except asyncio.CancelledError:
            raise
        except asyncio.IncompleteReadError as exc:
            if not await_response:
                self.logger.warning(f'{self.source_id} queue_send_to_connector_put connector api disconnected : {exc}')
        except ConnectionResetError:
            self.logger.warning(f'{self.source_id} queue_send_to_connector_put ConnectionResetError')            
        except Exception:
            self.logger.exception(f'{self.source_id} queue_send_to_connector_put')
            try:
                writer.close()
                if PYTHON_GREATER_37:
                    try:
                        await writer.wait_closed()      #python 3.7                                              
                    except Exception as exc:
                        self.logger.warning('queue_send_to_connector_put4 writer.wait_closed : '+str(exc))
                
            except Exception:
                pass
      
    def store_message_to_persistence_recv(self, msg_type, message, ignore_count=False):
        persistence_path = self.persistence_recv_path + msg_type
        try:
            new_size = len(message) + len(self.PERSISTENCE_SEPARATOR)
            if os.path.exists(persistence_path):
                new_size += os.path.getsize(persistence_path)
            if new_size > self.max_size_persistence_path:
                self.logger.warning(f'{self.source_id} Cannot store message of size {len(message)} to persistence_recv '
                                    f'for msg_type {msg_type} because {persistence_path} is too big')
                return
            self.logger.debug(f'{self.source_id} Storing message to persistence_recv for msg_type {msg_type}')
            if DEBUG_SHOW_DATA:
                self.logger.info('With data : '+str(message[200:210]))
            if self.PERSISTENCE_SEPARATOR in message:
                self.logger.warning(f'{self.source_id} message to persistence_recv for msg_type {msg_type} must be altered')
                message = message.replace(self.PERSISTENCE_SEPARATOR, self.PERSISTENCE_SEPARATOR_REPLACEMENT)                
            with open(persistence_path, mode='ab') as fd:
                fd.write(self.PERSISTENCE_SEPARATOR)
                #fd.write(peername.encode()+b'\n')
                fd.write(message)
            if self.debug_msg_counts and not ignore_count:
                #ignore_count is nice to understand we are in a Special case.
                #But msg_counts['load_persistence_recv'] will still contain duplicates
                self.msg_counts['store_persistence_recv'] += 1
                
        except Exception:
            self.logger.exception(f'{self.source_id} store_message_to_persistence_recv')        
          
    async def load_messages_from_persistence_recv(self, msg_type, dst_queue):
        self.logger.info(f'{self.source_id} Loading messages from persistence_recv {msg_type}')

       # async with self.persistence_lock:                       
        persistence_recv_path = self.persistence_recv_path + msg_type
        try:
            #if not os.path.exists(persistence_recv_path):
            #    self.logger.info('The persistence path '+persistence_recv_path+' does not exist')
            #    return
            persistent_count = 0
            fd = open(persistence_recv_path, mode='rb')             
            try:
                last_element = b''
                last_iteration = False                
                if fd.read(len(self.PERSISTENCE_SEPARATOR)) != self.PERSISTENCE_SEPARATOR:
                    self.logger.warning(f'Invalid persistence file {persistence_recv_path}')
                else:
                    while True:
                        #read file in chunks
                        chunk = fd.read(self.READ_PERSISTENCE_CHUNK_SIZE)
                        if not chunk:
                            last_iteration = True                    
                        chunk = last_element + chunk
                        #split chunks in message units to put to queue
                        persistence_units = chunk.split(self.PERSISTENCE_SEPARATOR)
                        if not last_iteration:
                            #last_element may be a partial separator to be completed in next read chunk                        
                            last_element = persistence_units[-1]
                            persistence_units = persistence_units[:-1]
                        for message in persistence_units:
                            self.logger.debug('Loading persistent_recv message to queue : '+msg_type)                 
                            message_tuple = self.unpack_message(message)
                            if DEBUG_SHOW_DATA:
                                self.logger.debug('With data : '+str(message_tuple[1][:10]))
                            if dst_queue.full():
                                self.logger.info(f'{self.source_id} : No room in dst_queue for more messages'
                                                 f' after persistent_count {persistent_count} messages')
                                last_iteration = True
                                break                                
                            await dst_queue.put(message_tuple)
                            persistent_count += 1
                            #sleep(0) is important otherwise queue_recv_from_connector may have losses under high loads
                            #because of this cpu intensive loop                            
                            await asyncio.sleep(0)#0.001)
                            if self.debug_msg_counts:
                                self.msg_counts['load_persistence_recv']+=1                            
    
                        if last_iteration:
                            break
            except Exception:
                self.logger.exception('open persistence_recv')
            finally:
                fd.close()            

            self.logger.info(f'{self.source_id} load_messages_from_persistence_recv finished loading {persistent_count}'
                             f' messages to transition_queue. Deleting persistence file {persistence_recv_path}')
            try:
                os.remove(persistence_recv_path)
            except Exception:
                self.logger.exception(f'{self.source_id} load_messages_from_persistence_recv delete persistence file')
        except asyncio.CancelledError:
            raise                            
        except Exception:
            self.logger.exception(f'{self.source_id} load_messages_from_persistence_recv')                    
            
    async def transition_to_persistent_recv(self, msg_type, message_bytes, persistence_recv_enabled, transition_queues):        
        disk_persistence_recv = self.disk_persistence_recv
        if isinstance(disk_persistence_recv, list):
            #disk_persistence can be a list of message types
            disk_persistence_recv = (msg_type in disk_persistence_recv)
        if disk_persistence_recv:
            self.logger.info(f'{self.source_id} queue_recv transition to persistence_recv for queue {msg_type}')
            if msg_type not in persistence_recv_enabled:
                persistence_recv_enabled.append(msg_type)
            self.store_message_to_persistence_recv(msg_type, message_bytes)                                    
            if msg_type in transition_queues:
                #in case disconnection happens during transition, copy queue remainings into new file
                transition_queue = transition_queues[msg_type]
                self.logger.info(f'{self.source_id} Special case : disconnection happens during transition for queue '
                                 f'{msg_type}. Transferring {transition_queue.qsize()} messages')                
                while not transition_queue.empty():
                    transport_json, data, binary = transition_queue.get_nowait()
                    message_bytes_queue = self.pack_message(transport_json=transport_json, data=data, binary=binary)
                    self.store_message_to_persistence_recv(msg_type, message_bytes_queue, ignore_count=True)
                del transition_queues[msg_type]
            
            
    async def queue_recv_from_connector(self):        
        self.logger.info(f'{self.source_id} queue_recv_from_connector task created')        
        #in case of error sending message to listener, the message type is added to persistence_recv_enabled
        #connectivity is retested for message type, and new messages are kept in persistence if still disconnected
        #if connectivity restores for message type, the message type and its new transition queue are stored in msg_types_ready_for_transition dict
        #all messages from persistence file are stored into msg_types_ready_for_transition queue, and file is deleted
        #if a transition queue is ready, it takes precedence over regular self.queue_recv
        #unfortunately, transition queues are emptied one after the other, not keeping the order between different message types.
        #special case : if disconnection happens during the transition to connectivity, 
        #we move all content from transition queue to a new recreated file
        TIMEOUT_QUEUE = 5
        persistence_recv_enabled = []    #list of msg_types
        transition_queues = {}    #key=msg_type, value=Queue of message_bytes
        ignore_redundant_log = False
        MAX_RESET_SOCKET = 100
        counter_reset_socket = 0
        
        while True:
            try:
                if persistence_recv_enabled:
                    #test if connectivity is back (per msg_type) : if yes, do transition from file to queue
                    msg_types_ready_for_transition = []
                    for msg_type in persistence_recv_enabled:
                        try:
                            uds_path_receive = self.uds_path_receive_from_connector.get(msg_type)                            
                            reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=uds_path_receive, 
                                                               limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                            writer.close()
                            if PYTHON_GREATER_37:
                                try:
                                    await writer.wait_closed()      #python 3.7                                              
                                except Exception as exc:
                                    self.logger.warning('queue_recv_from_connector1 writer.wait_closed : '+str(exc))
                            
                            msg_types_ready_for_transition.append(msg_type)
                        except Exception as exc: #ConnectionRefusedError:
                            if not ignore_redundant_log:
                                self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to '
                                                f'{uds_path_receive} : {exc}')
                    for msg_type in msg_types_ready_for_transition:
                        transition_queues[msg_type] = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
                        #read file into queue, delete file
                        await self.load_messages_from_persistence_recv(msg_type, transition_queues[msg_type])
                        persistence_recv_enabled.remove(msg_type)                                 
                 
                queue_recv_size = self.queue_recv.qsize()
                if queue_recv_size:
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : '
                                      f'{queue_recv_size}')                    
                    #if queue_recv_size > 10:
                    #    self.logger.info(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : {queue_recv_size}')
                    #else:
                    #    self.logger.debug(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : {queue_recv_size}')
                else:
                    if not ignore_redundant_log:
                        self.logger.debug(f'{self.source_id} queue_recv_from_connector wait for data')      
                
                if transition_queues:
                    #read from transition_queues, emptying one after the other, before coming back to self.queue_recv
                    msg_type_key = list(transition_queues.keys())[0]
                    transition_queue = transition_queues[msg_type_key]
                    #transport_json, data, binary = await transition_queue.get()
                    try:
                        transport_json, data, binary = await asyncio.wait_for(transition_queue.get(), timeout=TIMEOUT_QUEUE)
                        ignore_redundant_log = False
                    except asyncio.TimeoutError:
                        #let opportunity to check connectivity (persistence_recv_enabled) even when no event in queue
                        ignore_redundant_log = True
                        continue
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector Received transition message with : '
                                      f'{transport_json}')
                    if transition_queue.empty():
                        self.logger.info(f'Finished reading from transition queue : {msg_type_key}')
                        del transition_queues[msg_type_key]
                else:
                    #transport_json, data, binary = await self.queue_recv.get()
                    try:
                        transport_json, data, binary = await asyncio.wait_for(self.queue_recv.get(), timeout=TIMEOUT_QUEUE)
                        ignore_redundant_log = False
                    except asyncio.TimeoutError:
                        #let opportunity to check connectivity (persistence_recv_enabled) even when no event in queue                        
                        ignore_redundant_log = True
                        continue   
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector Received message with : '
                                      f'{transport_json}')
                    self.queue_recv.task_done()  #if someone uses 'join'
                
                uds_path_receive = self.uds_path_receive_from_connector.get(transport_json[MessageFields.MESSAGE_TYPE])
                if not uds_path_receive:
                    self.logger.error(f'{self.source_id} Invalid message type received by '
                                      f'queue_recv_from_connector {transport_json[MessageFields.MESSAGE_TYPE]}')
                    transport_json, data, binary = None, None, None
                    continue

                #here we could remove some fields from transport_json if better
                message_bytes = self.pack_message(transport_json=transport_json, data=data, binary=binary)
                
                msg_type = transport_json.get(MessageFields.MESSAGE_TYPE)
                if msg_type in persistence_recv_enabled:
                    #here, if persistence_recv_enabled has a msg_type element, 
                    #it means this msg_type has no connectivity, and must be kept in file                    
                    self.store_message_to_persistence_recv(msg_type, message_bytes)
                    continue
                
                if not os.path.exists(uds_path_receive):        
                    self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to '
                                        f'non existing {uds_path_receive}')
                    await self.transition_to_persistent_recv(msg_type, message_bytes, persistence_recv_enabled, 
                                                             transition_queues)
                    transport_json, data, binary = None, None, None
                    continue
        
                if self.uds_path_receive_preserve_socket:
                    #this requires start_waiting_for_messages in api to have a 
                    #client_connected_cb in a while loop, not a simple callback
                    use_existing_connection = False
                    counter_reset_socket += 1
                    #try to reuse connection to uds
                    if uds_path_receive in self.reader_writer_uds_path_receive:
                        if counter_reset_socket >= MAX_RESET_SOCKET:
                            #in case the api listener message_received_cb is very slow, the writer.drain may
                            #start blocking, so we workaround this by resetting the socket
                            counter_reset_socket = 0
                            self.logger.debug(f'Resetting reader_writer_uds_path_receive after {MAX_RESET_SOCKET} times')
                            writer = self.reader_writer_uds_path_receive[uds_path_receive][1]
                            writer.close()
                            if PYTHON_GREATER_37:
                                try:
                                    await writer.wait_closed()      #python 3.7                                              
                                except Exception as exc:
                                    self.logger.warning('queue_recv_from_connector2 writer.wait_closed : '+str(exc))
                            
                            del self.reader_writer_uds_path_receive[uds_path_receive]
                        else:
                            try:
                                writer = self.reader_writer_uds_path_receive[uds_path_receive][1]
                                writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])    
                                writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])                        
                                await writer.drain()                                        
                                use_existing_connection = True
                                self.logger.debug(f'{self.source_id} queue_recv_from_connector reusing existing connection')
                            except Exception:
                                del self.reader_writer_uds_path_receive[uds_path_receive]
                                self.logger.exception('queue_recv_from_connector')
                            
                    if not use_existing_connection:
                        self.logger.debug(f'{self.source_id} queue_recv_from_connector creating new connection')
                        try:
                            self.reader_writer_uds_path_receive[uds_path_receive] = reader, writer = await \
                                                asyncio.wait_for(asyncio.open_unix_connection(path=uds_path_receive, 
                                                        limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                            writer.transport.set_write_buffer_limits(0,0)
                        except asyncio.CancelledError:
                            raise                                                
                        except Exception as exc: #ConnectionRefusedError:
                            self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect '
                                                f'to {uds_path_receive} : {exc}')
                            await self.transition_to_persistent_recv(msg_type, message_bytes, 
                                                                     persistence_recv_enabled, transition_queues)
                            transport_json, data, binary = None, None, None
                            continue                        
                        writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])                                                                
                        writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
#                       await asyncio.wait_for(writer.drain(), timeout=self.ASYNC_TIMEOUT)                                       
                        await writer.drain()                
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector finished sending data '
                                      f'to {uds_path_receive}')                    
                    
                else:
                    #this requires start_waiting_for_messages in api to have a 
                    #client_connected_cb as a simple callback, not in a while loop
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector creating new connection')
                    try:
                        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=uds_path_receive, 
                                                           limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                        writer.transport.set_write_buffer_limits(0,0)
                    except asyncio.CancelledError:
                        raise                        
                    except Exception as exc: #ConnectionRefusedError:
                        self.logger.warning(f'{self.source_id} queue_recv_from_connector could not '
                                            f'connect to {uds_path_receive} : {exc}')
                        await self.transition_to_persistent_recv(msg_type, message_bytes, 
                                                                 persistence_recv_enabled, transition_queues)                                
                        transport_json, data, binary = None, None, None
                        continue                        
                    writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])                                                                
                    writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
                    await writer.drain()                                    
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector finished sending '
                                      f'data to {uds_path_receive}')                                        
                    writer.close()                                       
                    if PYTHON_GREATER_37:
                        try:
                            await writer.wait_closed()      #python 3.7                                              
                        except Exception as exc:
                            self.logger.warning('queue_recv_from_connector3 writer.wait_closed : '+str(exc))

                message_bytes, transport_json, data, binary = None, None, None, None                
                #await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception(f'{self.source_id} queue_recv_from_connector')
                try:
                    writer.close()
                    if PYTHON_GREATER_37:
                        try:
                            await writer.wait_closed()      #python 3.7                                              
                        except Exception as exc:
                            self.logger.warning('queue_recv_from_connector4 writer.wait_closed : '+str(exc))
                    
                except Exception:
                    self.logger.exception('queue_recv_from_connector writer close')
            
    async def run_server(self):
        self.logger.info('Running server')
        try:
            ssl_context = self.build_server_ssl_context() if self.use_ssl else None
            self.server = await asyncio.start_server(self.manage_full_duplex, sock=self.sock, ssl=ssl_context,
                                                     limit=self.MAX_SOCKET_BUFFER_SIZE)
            if self.use_ssl:
                if self.ssl_allow_all:
                    self.logger.info('server is running with ssl, allow all')
                else:
                    self.logger.info('server is running with ssl')
            else:
                self.logger.info('server is running without ssl')
        except asyncio.CancelledError:
            raise            
        except Exception:
            self.logger.exception('run_server')
            
        return self.server
    
    async def run_client(self, alternate_client_cert_toggle_default=False):
        #alternate_client_cert_toggle_default is used only by start() called by client_wait_for_reconnect
        if self.use_ssl:
            if self.ssl_allow_all:
                self.client_certificate_name = None
                self.logger.info(f'{self.source_id} Running client with certificate {self.client_certificate_name}, allow all')
            else:
                if os.path.exists(self.ssl_helper.CLIENT_PEM_PATH.format(self.source_id)):
                    if alternate_client_cert_toggle_default:
                        self.client_certificate_name = self.ssl_helper.CLIENT_DEFAULT_CERT_NAME
                        self.logger.info('Client alternating with the default certificate : '+self.client_certificate_name)
                    else:                        
                        self.client_certificate_name = self.source_id
                        self.logger.info('Client will use a unique certificate : '+self.client_certificate_name)                
                else:
                    self.client_certificate_name = self.ssl_helper.CLIENT_DEFAULT_CERT_NAME
                    self.logger.info('Client will use the default certificate : '+self.client_certificate_name)
                
                self.logger.info(f'{self.source_id} Running client with certificate {self.client_certificate_name}')
        else:
            self.client_certificate_name = None
            self.logger.info(f'{self.source_id} Running client without ssl')
        try:
            ssl_context = self.build_client_ssl_context() if self.use_ssl else None        
            server_hostname = '' if self.use_ssl else None
            if self.client_cafile_verify_server or self.token_client_verify_server_hostname: 
                #here we assume server_sockaddr[0] is a hostname, not an ip address.
                #this should go together with client_cafile_verify_server or token_verify_peer_cert='/etc/ssl/certs/ca-certificates.crt'
                server_hostname = self.server_sockaddr[0]
                #self.inside_end_sockpair is not None only when proxy with ssl_server is configured
                self.logger.info(f'{self.source_id} Client will connect to server {server_hostname}')            
            socket_used = self.inside_end_sockpair or self.sock
            reader, writer = await asyncio.wait_for(asyncio.open_connection(sock=socket_used, ssl=ssl_context,
                                    server_hostname=server_hostname, limit=self.MAX_SOCKET_BUFFER_SIZE), 
                                    timeout=self.ASYNC_TIMEOUT)
            writer.transport.set_write_buffer_limits(0,0)                                                                 
            await self.manage_full_duplex(reader, writer)
            self.logger.info(f'{self.source_id} client is running')            
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception(f'{self.source_id} run_client')
            self.tasks['client_wait_for_reconnect'] = self.loop.create_task(self.client_wait_for_reconnect())    


    async def manage_full_duplex(self, reader, writer):
        full_duplex = FullDuplex(weakref.proxy(self), reader, writer)
        
        if self.is_server:
            self.logger.info(f'{self.source_id} manage_full_duplex received new connection from '
                             f'client {writer.get_extra_info("peername")}')            
        else:
            self.logger.info(f'{self.source_id} manage_full_duplex Starting connection')            
        self.full_duplex_connections[str(writer.get_extra_info("peername"))] = full_duplex
        await full_duplex.start()


    def override_server_ssl_context(self, ssl_socket, server_name, ssl_context):
        try:
            self.logger.debug(f'{self.source_id} overriding server ssl context at new connection')
            new_context = self.build_server_ssl_context()
            ssl_socket.context = new_context
        except Exception:
            self.logger.exception('override_server_ssl_context')
        
    def build_server_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        #test using : curl -I -v --tlsv1.2 --tls-max 1.2 https://localhost:10673
        if self.server_secure_tls:
            if PYTHON_GREATER_37:
                context.minimum_version = ssl.TLSVersion.TLSv1_2
            else:
                context.options = ssl.OP_NO_TLSv1_1
        
        if self.ssl_allow_all:
            if self.use_token and self.token_verify_peer_cert:
                context.verify_mode = ssl.CERT_REQUIRED
                context.load_cert_chain(certfile=self.ssl_helper.SERVER_PEM_PATH, keyfile=self.ssl_helper.SERVER_KEY_PATH)
                context.load_verify_locations(capath=self.ssl_helper.SERVER_SYMLINKS_PATH)
            else:
                context.verify_mode = ssl.CERT_NONE
                context.load_cert_chain(certfile=self.ssl_helper.SERVER_PEM_PATH, keyfile=self.ssl_helper.SERVER_KEY_PATH)
            
        else:
            context.verify_mode = ssl.CERT_REQUIRED
            # sni_callback : happens at each new connection.
            #the override enables to create a new and updated server ssl context each time a new client connects
            #necessary when deleting a client certificate on server : in order to remove it also from the context (ram)
            #OBSOLETE : this is necessary because after the new client certificate has been generated and written into SERVER_SYMLINKS_PATH, it
            #is not yet loaded automatically into the current context : so when the client reconnects to the server with its new
            #certificate, the context needs to be rebuilt in order to call again load_verify_locations(capath, which will
            #take into account the new certificate.
            context.load_cert_chain(certfile=self.ssl_helper.SERVER_PEM_PATH, keyfile=self.ssl_helper.SERVER_KEY_PATH)
            if self.server_ca:
                #just validate that the client cert is signed by our server CA. Then let FullDuplex start() validate
                #that peer_cert['serialNumber'] is in source_id_2_cert
                #flexibility to manually add defaultN or CA certificates under SERVER_SYMLINKS_PATH
                #context.load_verify_locations(cafile=self.ssl_helper.SERVER_CA_PEM_PATH)
                context.load_verify_locations(capath=self.ssl_helper.SERVER_SYMLINKS_PATH)                
            else:
                context.load_verify_locations(capath=self.ssl_helper.SERVER_SYMLINKS_PATH)
                
            try:
                if PYTHON_GREATER_37:
                    context.sni_callback = self.override_server_ssl_context
                else:
                    context.set_servername_callback(self.override_server_ssl_context)            
            except Exception:
                self.logger.exception('build_server_ssl_context')
        return context
    

    def build_client_ssl_context(self):
        if self.ssl_allow_all:
            if self.use_token:
                if self.token_verify_peer_cert:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)            
                else:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                if self.token_client_send_cert:
                    context.load_cert_chain(certfile=self.ssl_helper.CLIENT_PEM_PATH.format(self.ssl_helper.CLIENT_DEFAULT_CERT_NAME),
                                            keyfile=self.ssl_helper.CLIENT_KEY_PATH.format(self.ssl_helper.CLIENT_DEFAULT_CERT_NAME))                
                if self.token_verify_peer_cert:
                    context.verify_mode = ssl.CERT_REQUIRED        
                    if self.token_verify_peer_cert is True:
                        if not os.path.exists(self.ssl_helper.CLIENT_SERVER_CRT_PATH):
                            #support old .crt name from < 1.2.0
                            context.check_hostname = False
                            context.load_verify_locations(cafile=self.ssl_helper.CLIENT_SERVER_CRT_PATH.replace('.pem','.crt'))
                        else:
                            context.load_verify_locations(cafile=self.ssl_helper.CLIENT_SERVER_CRT_PATH)                   
                    else:
                        #cafile can be like : "/etc/ssl/certs/ca-certificates.crt"
                        context.load_verify_locations(cafile=self.token_verify_peer_cert)
                else:
                    context.verify_mode = ssl.CERT_NONE
            else:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)                
                context.verify_mode = ssl.CERT_NONE                    
        else:        
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)            
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = False
            context.load_cert_chain(certfile=self.ssl_helper.CLIENT_PEM_PATH.format(self.client_certificate_name),
                                    keyfile=self.ssl_helper.CLIENT_KEY_PATH.format(self.client_certificate_name))
            #in case server certificate change, client should first replace/chain the new server certificate in its cafile
            if self.client_cafile_verify_server:
                #cafile can be like : "/etc/ssl/certs/ca-certificates.crt"
                #server can be tested with : openssl s_client --connect <server_ip>:<server_port>
                context.check_hostname = True
                context.load_verify_locations(cafile=self.client_cafile_verify_server)
            elif not os.path.exists(self.ssl_helper.CLIENT_SERVER_CRT_PATH):
                #support old .crt name from < 1.2.0
                context.load_verify_locations(cafile=self.ssl_helper.CLIENT_SERVER_CRT_PATH.replace('.pem','.crt'))
            else:
                context.load_verify_locations(cafile=self.ssl_helper.CLIENT_SERVER_CRT_PATH)
            #we might want to chain multiple certificates in CLIENT_SERVER_CRT_PATH, to support multiple server certificates
        return context


