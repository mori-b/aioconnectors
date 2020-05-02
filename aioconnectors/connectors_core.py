'''
Protocol between client and server : 
4 bytes = struct 'I' for size up to 2**32-1 = 4294967295 = 4GB
2 bytes = struct 'H' for size up to 2**16-1 = 65536 = 65 KB

network layer : 
4 bytes = length, rest is data 

transport layer :
2 bytes = length, rest is json descriptor like : {'message_type':['_ssl','command','event'(,'file')], 'source_id':'gid:uid', 'destination_id':'<tsoc_ip>, ['request_id', 'response_id']:<int>, 'with_binary':true}
request_id and response_id are relevant for application layer

application layer :
4 bytes = length of json following, rest is custom json
if with_binary key exists :
4 bytes = length of binary data following, rest is binary data

'''

import asyncio
from struct import Struct
import json
import logging
import sys
import time
import socket
import weakref
import os
import ssl
import uuid
from copy import deepcopy

from .ssl_helper import SSL_helper

#logging.basicConfig(level=logging.DEBUG)

PYTHON_VERSION = (sys.version_info.major,sys.version_info.minor)
if PYTHON_VERSION < (3,6):
    print('aioconnectors minimum requirement : Python 3.6')
    sys.exit(1)
PYTHON_GREATER_37 = (PYTHON_VERSION >= (3,7))
    
    
DEBUG_SHOW_DATA = False
DEFAULT_LOGGER_NAME = 'aioconnector'
LOGFILE_DEFAULT_PATH = 'aioconnectors.log'
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_SHORT = '%(asctime)s - %(levelname)s - %(message)s'

class MessageFields:
    MESSAGE_TYPE = 'message_type'    #'_ssl', '_ack', '_ping', '_handshake_ssl', '_handshake_no_ssl', <user-defined>, ...
    SOURCE_ID = 'source_id'    #str
    DESTINATION_ID = 'destination_id'    #str
    REQUEST_ID = 'request_id'    #int
    RESPONSE_ID = 'response_id'    #int
    WITH_BINARY = 'with_binary'    #boolean
    AWAIT_RESPONSE = 'await_response'    #boolean
    WITH_FILE = 'with_file'    #dict {'src_path':<str>, 'dst_name':<str>, 'dst_type':<str>, 'binary_offset':<int>, 'delete':<boolean>}
    TRANSPORT_ID = 'transport_id'    #int
    WAIT_FOR_ACK = 'wait_for_ack'    #boolean
    
class Structures:
    MSG_4_STRUCT = Struct('I')    #4
    MSG_2_STRUCT = Struct('H')    #2
#MSG_LENGTH_STRUCT = Struct('Q')    #8

def get_logger(logfile_path=LOGFILE_DEFAULT_PATH, first_run=False, silent=True, logger_name=DEFAULT_LOGGER_NAME, log_format=LOG_FORMAT, level=LOG_LEVEL):
    logger = logging.getLogger(logger_name)
    logger.handlers = []
    if not first_run:
        handlers = []
        if logfile_path:    #could be '' if no config file provided
            handlers.append(logging.FileHandler(logfile_path))
        if not silent:
            handlers.append(logging.StreamHandler(sys.stdout))
        if not handlers:
            logger.addHandler(logging.NullHandler())
            return logger

        log_level = getattr(logging, level, logging.INFO)
        logger.setLevel(log_level)        
        
        formatter = logging.Formatter(log_format)
        formatter.converter = time.gmtime
        for fh in handlers:
            fh.setFormatter(formatter)
            fh.setLevel(log_level)        
            logger.addHandler(fh)
    else:
        logger.addHandler(logging.NullHandler())    
    
    return logger

def full_path(the_path):
    if the_path is not None:
        return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))


class Connector:
    ############################################
    #default values configurable at __init__
    SERVER_ADDR =  ('127.0.0.1',10673)
    USE_SSL = True
    CONNECTOR_FILES_DIRPATH = '/tmp/aioconnectors'
    SERVER_VALIDATES_CLIENTS = True    
    DISK_PERSISTENCE_SEND = False    #can be boolean, or list of message types having disk persistence enabled
    #RAM_PERSISTENCE cannot be true in the current implementation, since queue_send[peername] doesn't exist anymore in disconnected mode
    #however the code still exists partially, in case of future change
    PERSISTENCE_SEND_FILE_NAME = 'connector_disk_persistence_send_from_{}_to_peer'
    DISK_PERSISTENCE_RECV = False
    PERSISTENCE_RECV_FILE_NAME = 'connector_disk_persistence_recv_by_{}'    
    MAX_SIZE_PERSISTENCE_PATH = 1000000000 #1gb
    UDS_PATH_RECEIVE_PRESERVE_SOCKET = True
    UDS_PATH_SEND_PRESERVE_SOCKET = True    
    SILENT=True    
    #USE_ACK = False  #or can be a list of message_types like ['type1']    
    DEBUG_MSG_COUNTS = True
    
    ############################################    
    #default values of internals (not configurable at __init__)
    SLEEP_BETWEEN_START_FAILURES = 5 #10
    ASYNC_TIMEOUT = 10
    MAX_QUEUE_SIZE = 10_000_000 #10_000_000 #8192
    AWAIT_RESPONSE_TIMEOUT = 3600 #1 hour
    MAX_NUMBER_OF_AWAITING_REQUESTS = 1000
    MAX_SOCKET_BUFFER_SIZE = 2 ** 16    #2 ** 16 is asyncio default
    MAX_TRANSPORT_ID = 100000
    MAX_RETRIES_BEFORE_ACK = 3
    DEFAULT_MESSAGE_TYPES = ['any']   
    PERSISTENCE_SEPARATOR = b'@@@PERSISTENCE_SEPARATOR@@@'
    FILE_TYPE2DIRPATH = {}    #example : {'documents':'/tmp/documents', 'executables':'/tmp/executables'}
    DELETE_CLIENT_PRIVATE_KEY_ON_SERVER = False
    UDS_PATH_COMMANDER = 'uds_path_commander_{}'   
    
    UDS_PATH_SEND_TO_CONNECTOR_SERVER = 'uds_path_send_to_connector_server_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER = 'uds_path_receive_from_connector_server_{}_{}'    

    UDS_PATH_SEND_TO_CONNECTOR_CLIENT = 'uds_path_send_to_connector_client_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT = 'uds_path_receive_from_connector_client_{}_{}'    
    
    
    def __init__(self, logger, server_sockaddr=SERVER_ADDR, is_server=True, client_name=None, client_bind_ip=None,
                 use_ssl=USE_SSL, ssl_allow_all=False, certificates_directory_path=None, validates_clients=SERVER_VALIDATES_CLIENTS,
                 disk_persistence_send=DISK_PERSISTENCE_SEND, disk_persistence_recv=DISK_PERSISTENCE_RECV,
                 max_size_persistence_path=MAX_SIZE_PERSISTENCE_PATH, #use_ack=USE_ACK,
                 send_message_types=None, recv_message_types=None, tool_only=False, file_type2dirpath=None,
                 debug_msg_counts=DEBUG_MSG_COUNTS, silent=SILENT, connector_files_dirpath = CONNECTOR_FILES_DIRPATH,
                 uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET,
                 hook_server_auth_client=None, enable_client_try_reconnect=True):
        
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
                self.msg_counts = {'store_persistence_send':0, 'load_persistence_send':0, 'send_no_persist':0, 'queue_sent':0,'queue_recv':0, 'msg_recvd_uds':0, 'store_persistence_recv':0, 'load_persistence_recv':0}
                self.previous_msg_counts = deepcopy(self.msg_counts)
            
            self.server_sockaddr = server_sockaddr
            self.is_server = is_server            
            self.use_ssl, self.ssl_allow_all, self.certificates_directory_path = use_ssl, ssl_allow_all, full_path(certificates_directory_path)
            self.server = self.send_to_connector_server = None

            if self.is_server:
                self.source_id = str(self.server_sockaddr)
                self.logger.info('Server has source id : '+self.source_id)                
                self.alnum_source_id = '_'.join([self.alnum_name(el) for el in self.source_id.split()])
                self.hook_server_auth_client = hook_server_auth_client

                if send_message_types is None:
                    send_message_types = self.DEFAULT_MESSAGE_TYPES
                if recv_message_types is None:
                    recv_message_types = self.DEFAULT_MESSAGE_TYPES
                self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
                
                self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, self.UDS_PATH_SEND_TO_CONNECTOR_SERVER.format(self.alnum_source_id))
                self.uds_path_receive_from_connector = {}
                for recv_message_type in self.recv_message_types:
                    self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, self.UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER.format(recv_message_type, self.alnum_source_id))
                
            else:
                self.client_bind_ip = client_bind_ip
                self.source_id = client_name
                self.logger.info('Client has source id : '+self.source_id)   
                self.alnum_source_id = self.alnum_name(self.source_id)                            
                self.enable_client_try_reconnect = enable_client_try_reconnect
                
                if send_message_types is None:
                    send_message_types = self.DEFAULT_MESSAGE_TYPES
                if recv_message_types is None:
                    recv_message_types = self.DEFAULT_MESSAGE_TYPES
                self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
                
                self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, self.UDS_PATH_SEND_TO_CONNECTOR_CLIENT.format(self.alnum_source_id))
                self.uds_path_receive_from_connector = {}           
                for recv_message_type in self.recv_message_types:
                    self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, self.UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT.format(recv_message_type, self.alnum_source_id))
            
            self.commander_server = self.commander_server_task = None
            self.uds_path_commander = os.path.join(self.connector_files_dirpath, self.UDS_PATH_COMMANDER.format(self.alnum_source_id))
                    
            if not tool_only:
                
                if self.is_server:
                    self.validates_clients = validates_clients
                    self.full_duplex_connections = []                
                else:
                    self.client_certificate_name = None     
                    self.full_duplex = None
                    
                
                if self.use_ssl:                    
                    self.ssl_helper = SSL_helper(self.logger, self.is_server, self.certificates_directory_path)
                    self.logger.info('Connector will use ssl, with certificates directory '+self.ssl_helper.certificates_base_path)

                    if self.is_server:                
                        pass
                    elif not self.ssl_allow_all:
                        if os.path.exists(self.ssl_helper.CLIENT_PEM_PATH.format(self.source_id)):
                            self.client_certificate_name = self.source_id
                            self.logger.info('Client will use a unique certificate : '+self.client_certificate_name)                
                        else:
                            self.client_certificate_name = self.ssl_helper.CLIENT_DEFAULT_CERT_NAME
                            self.logger.info('Client will use the default certificate : '+self.client_certificate_name)
                            
                else:                        
                    self.logger.info('Connector will not use ssl')                                                               
                    
                self.disk_persistence = disk_persistence_send
                self.persistence_path = os.path.join(self.connector_files_dirpath, self.PERSISTENCE_SEND_FILE_NAME.format(self.alnum_source_id) + '_') #peer name will be appended
                self.disk_persistence_recv = disk_persistence_recv
                self.persistence_recv_path = os.path.join(self.connector_files_dirpath, self.PERSISTENCE_RECV_FILE_NAME.format(self.alnum_source_id) + '_') #msg type will be appended         
                self.ram_persistence = False #ram_persistence    
                self.max_size_persistence_path = max_size_persistence_path
                if self.disk_persistence:
                    if self.persistence_path:
                        self.logger.info('Connector will use send persistence path {} with max size {}'.format(self.persistence_path, self.max_size_persistence_path))
                    else:
                        self.logger.warning('Connector misconfigured : disk_persistence is enabled without persistence_path send')
                        #self.logger.info('Connector will use persistence ram')
                else:
                    self.logger.info('Connector will not use persistence send')                                

                if self.disk_persistence_recv:
                    if self.persistence_recv_path:
                        self.logger.info('Connector will use recv persistence path {} with max size {}'.format(self.persistence_recv_path, self.max_size_persistence_path))
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
                
                if file_type2dirpath is None:
                    self.file_type2dirpath = self.FILE_TYPE2DIRPATH
                else:
                    self.file_type2dirpath = file_type2dirpath
                    
                self.ignore_peer_traffic = False
                self.loop = asyncio.get_event_loop()
                #commander_server lives besides start/stop
                self.commander_server_task = self.loop.create_task(self.create_commander_server())
                    
        except Exception:
            self.logger.exception('init')
            raise

    async def create_commander_server(self):            
        self.commander_server = await asyncio.start_unix_server(self.commander_cb, path=self.uds_path_commander)
            
                
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
            if queues_stats['queue_recv'] or any(queues_stats['queue_send'].values()):
                self.logger.info('peek_queues : '+json.dumps(queues_stats, indent=4, sort_keys=True))
                
    def alnum_name(self, name):
        return ''.join([letter for letter in name if letter.isalnum()])
                
    
    async def start(self, connector_socket_only=False):
        #connector_socket_only is used by client_wait_for_reconnect
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
                if self.debug_msg_counts:
                    self.tasks['log_msg_counts'] = self.loop.create_task(self.log_msg_counts())                
                self.reader_writer_uds_path_receive = {}
                self.tasks['queue_recv_from_connector'] = self.loop.create_task(self.queue_recv_from_connector())
                self.tasks['queue_send_to_connector'] = self.loop.create_task(self.queue_send_to_connector())                
            
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
            
            if self.is_server:
                self.sock.setblocking(False)
                self.sock.bind(self.server_sockaddr)
                self.tasks['run_server'] = self.loop.create_task(self.run_server())
            else:
                if self.client_bind_ip:
                    self.sock.bind((self.client_bind_ip,0))
                self.sock.connect(self.server_sockaddr)   
                self.sock.setblocking(False)                
                self.logger.info('Created socket for '+self.source_id+' with info '+str(self.sock.getsockname())+' to peer '+str(self.sock.getpeername()))
                
                self.tasks['run_client'] = self.loop.create_task(self.run_client())    
                #self.logger.info('ALL TASKS : '+str(self.tasks['run_client'].all_tasks()))            
            return
        except ConnectionRefusedError as exc:
            self.logger.warning(str(exc))
            if not self.is_server:
                self.tasks['client_wait_for_reconnect'] = self.loop.create_task(self.client_wait_for_reconnect())    
            return                
        except Exception:
            self.logger.exception('start')
            self.loop.create_task(self.restart(sleep_between=self.SLEEP_BETWEEN_START_FAILURES))
            return
        
    async def stop(self, connector_socket_only=False, client_wait_for_reconnect=False, hard=False, shutdown=False):
            
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
                for full_duplex in self.full_duplex_connections:
                    await full_duplex.stop(hard=hard)
                self.full_duplex_connections = []
                try:
                    if self.server:
                        self.server.close()
                        await self.server.wait_closed()
                except Exception:
                    self.logger.exception('stop server')
            else:
                if not client_wait_for_reconnect:
                    if self.full_duplex:
                        await self.full_duplex.stop(hard=hard)

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
                    if os.path.exists(self.uds_path_commander):                
                        self.logger.info('Deleting file '+self.uds_path_commander)                    
                        os.remove(self.uds_path_commander)
                    self.logger.info('Destroying ConnectorManager '+self.source_id)                    
            except Exception:
                self.logger.exception('shutdown : remove uds_path_commander')
        
                
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
            self.logger.info('cancel_tasks called. Current self.tasks are : '+str(list(self.tasks.keys())))
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
                self.sock.connect(self.server_sockaddr)            
            except ConnectionRefusedError:
                self.logger.error(f'{self.source_id} client_wait_for_reconnect failed connection attempt number {count}')
                count += 1
                continue
            finally:
                try:
                    self.sock.shutdown()                    
                    self.sock.close()
                except Exception:
                    pass
            
            self.logger.info(f'{self.source_id} client_wait_for_reconnect connectivity was reestablished !')
            
            await self.start(connector_socket_only=True)
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
                    response = b'success'    #b'{}'
                response = Structures.MSG_4_STRUCT.pack(len(response)) + response
                self.logger.info(f'commander_cb responding : {response}')
                writer.write(response)            
                try:
                    await asyncio.wait_for(writer.drain(), timeout=Connector.ASYNC_TIMEOUT)
                except Exception:
                    self.logger.exception('commander_cb writer drain')     
            writer.close()
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
        res = []
        if self.queue_send:
            res = list(sorted(self.queue_send.keys()))
        if dump_result:
            return str(res)
        return res

    def manage_ignore_peer_traffic(self, show=False, enable=False, disable=False, unique_peer=False):
        if show:
            return str(self.ignore_peer_traffic)
        if enable:
            self.ignore_peer_traffic = True
            return str(self.ignore_peer_traffic)
        if disable:
            self.ignore_peer_traffic = False
            return str(self.ignore_peer_traffic)
        if unique_peer:
            self.ignore_peer_traffic = [unique_peer]
            return str(self.ignore_peer_traffic)

    async def delete_client_certificate_on_server(self, client_id=None, remove_only_symlink=False):
        try:
            response = await self.ssl_helper.remove_client_cert_on_server(client_id, remove_only_symlink=remove_only_symlink)
            return response
        except Exception as exc:
            self.logger.exception('delete_client_certificate_on_server')
            return json.dumps({'status':False, 'msg':str(exc)})

    async def delete_client_certificate_on_client(self):
        try:
            response = await self.ssl_helper.remove_client_cert_on_client(self.source_id)
            return response
        except Exception as exc:
            self.logger.exception('delete_client_certificate_on_client')
            return json.dumps({'status':False, 'msg':str(exc)})


    def delete_previous_persistence_remains(self):
        try:
            self.logger.info(f'{self.source_id} delete_previous_persistence_remains checking files')
            
            persistence_dir = os.path.dirname(self.persistence_path)
            persistence_basename = os.path.basename(self.persistence_path)
            for filename in os.listdir(persistence_dir):            
                if filename.startswith(persistence_basename):
                    old_persistence_path = os.path.join(persistence_dir, filename)
                    self.logger.warning(f'{self.source_id} delete_previous_persistence_remains Deleting old persistent file : {old_persistence_path}')
                    os.remove(old_persistence_path)
                    
            persistence_recv_dir = os.path.dirname(self.persistence_recv_path)
            persistence_recv_basename = os.path.basename(self.persistence_recv_path)
            for filename in os.listdir(persistence_recv_dir):            
                if filename.startswith(persistence_recv_basename):
                    old_persistence_path = os.path.join(persistence_recv_dir, filename)
                    self.logger.warning(f'{self.source_id} delete_previous_persistence_remains Deleting old persistent file : {old_persistence_path}')
                    os.remove(old_persistence_path)
                    
        except Exception:
            self.logger.exception(f'{self.source_id} delete_previous_persistence_remains')
                
    def store_message_to_persistence(self, peername, message, ignore_count=False):
        persistence_path = self.persistence_path + self.alnum_name(peername)
        try:
            if os.path.exists(persistence_path) and os.path.getsize(persistence_path) > self.max_size_persistence_path:
                self.logger.warning(f'{self.source_id} Cannot store message to persistence for peer {peername} because {persistence_path} is too big')
                return
            self.logger.debug(f'{self.source_id} Storing message to persistence to peer {peername}')
            if DEBUG_SHOW_DATA:
                self.logger.info('With data : '+str(message[200:210]))
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
            persistence_content = None
            with open(persistence_path, mode='rb') as fd:
                persistence_content = fd.read()
            persistent_count = 0
            if persistence_content:
                persistence_units = persistence_content.split(self.PERSISTENCE_SEPARATOR)[1:]
                persistence_content = None
                for message in persistence_units:
                    self.logger.debug('Loading persistent message to queue : '+peername)                 
                    message_tuple = self.unpack_message(message)
                    if DEBUG_SHOW_DATA:
                        self.logger.debug('With data : '+str(message_tuple[1][:10]))                                       
                    await queue_send.put(message_tuple)
                    persistent_count += 1
                    #sleep(0) is important otherwise queue_send_to_connector_put may have losses under high loads
                    #because of this cpu intensive loop
                    await asyncio.sleep(0)#0.001)
                    if self.debug_msg_counts:
                        self.msg_counts['load_persistence_send']+=1                            

            self.logger.info(f'{self.source_id} load_messages_from_persistence finished loading {persistent_count} messages to queue_send_transition_to_connect. Deleting persistence file {persistence_path}')
            try:
                os.remove(persistence_path)
            except Exception:
                self.logger.exception(f'{self.source_id} load_messages_from_persistence delete persistence file')
                            
        except Exception:
            self.logger.exception(f'{self.source_id} load_messages_from_persistence')        
        
        
    #4|2|json|4|data|4|binary
    def pack_message(self, transport_json=None, message_type=None, source_id=None, destination_id=None,
                     request_id=None, response_id=None, binary=None, await_response=False,
                     with_file=None, data=None, wait_for_ack=False):
        if DEBUG_SHOW_DATA:
            self.logger.debug('pack_message with params : '+str(message_type)+', '+str(data)+', '+str(transport_json))
        if transport_json is None:
            transport_json = {MessageFields.MESSAGE_TYPE : message_type or self.send_message_types[0]}
            if source_id:
                transport_json[MessageFields.SOURCE_ID] = source_id
            if destination_id:
                transport_json[MessageFields.DESTINATION_ID] = destination_id           
            if request_id:
                transport_json[MessageFields.REQUEST_ID] = request_id
            if response_id:
                transport_json[MessageFields.RESPONSE_ID] = response_id            
            if binary:
                transport_json[MessageFields.WITH_BINARY] = True
            if await_response:
                transport_json[MessageFields.AWAIT_RESPONSE] = True    
            if with_file:
                transport_json[MessageFields.WITH_FILE] = with_file
            if wait_for_ack:
                transport_json[MessageFields.WAIT_FOR_ACK] = wait_for_ack
                
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
        server = self.send_to_connector_server = await asyncio.start_unix_server(self.queue_send_to_connector_put, path=self.uds_path_send_to_connector, limit=self.MAX_SOCKET_BUFFER_SIZE)
        return server

    async def queue_send_to_connector_put(self, reader, writer):
        # receives from uds socket, writes to queue_send
        #4|2|json|4|data|4|binary        
        try:
            while True:
                if self.debug_msg_counts:
                    self.msg_counts['msg_recvd_uds'] += 1            
                self.logger.debug(f'{self.source_id} Receiving data from unix clients to queue_send_to_connector')
                next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
                next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
                #payload = 2|json|4|data|4|binary
                payload = await asyncio.wait_for(reader.readexactly(next_length), timeout=self.ASYNC_TIMEOUT)
                message = next_length_bytes + payload
                message_tuple = transport_json , data, binary = self.unpack_message(message)
                
                peername = transport_json.get(MessageFields.DESTINATION_ID)
                if not self.is_server:
                    if not peername:
                        transport_json[MessageFields.DESTINATION_ID] = peername = self.server_sockaddr 
                    elif peername != str(self.server_sockaddr):
                        self.logger.warning(f'{self.source_id} queue_send_to_connector_put : overriding invalid destination id {peername} instead of {self.server_sockaddr}')
                        transport_json[MessageFields.DESTINATION_ID] = peername = self.server_sockaddr
                
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
                        await asyncio.sleep(0)
                        if not self.uds_path_send_preserve_socket:
                            return
                        continue
                        
                #sanity check, that source_id exists and is valid : that way connectors_api doesn't have to ensure source_id, and cannot modify it
                #but still it is more efficient to rely on connectors_api source_id
                if transport_json.get(MessageFields.SOURCE_ID) != self.source_id:
                    self.logger.warning(f'{self.source_id} queue_send_to_connector_put : overriding invalid source id {transport_json.get(MessageFields.SOURCE_ID)} instead of {self.source_id}')
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
                        self.logger.debug(f'{self.source_id} queue_send_to_connector_put storing message to persistence for queue {peername}')
                        send_to_queue = False                                
                        self.store_message_to_persistence(peername=peername, message=message)      
                    elif not self.ram_persistence:
                        #self.logger.info('Not adding message to queue_send_to_connector_put because ram_persistence is false')                        
                        self.logger.debug(f'{self.source_id} Not forwarding message to absent peer {peername} to queue_send_to_connector_put because no persistence')                    
                        send_to_queue = False
                            
                message_type = transport_json[MessageFields.MESSAGE_TYPE]
                if (message_type not in self.send_message_types) and (message_type != '_ping'):
                    self.logger.warning(f'{self.source_id} queue_send_to_connector_put received a message with invalid type {message_type}. Ignoring...')                    
                    send_to_queue = False                    
                elif transport_json.get(MessageFields.AWAIT_RESPONSE):
                    request_id = transport_json.get(MessageFields.REQUEST_ID)
                    if not request_id:
                        self.logger.warning(f'{self.source_id} sending message with AWAIT_RESPONSE enabled, but without request_id')
                        #if no application level request_id has been set, we create a dummy unique request_id to be able to detect response
                        #in case peer's application responds smartly, otherwise AWAIT_RESPONSE_TIMEOUT will stop the waiting
                        request_id = uuid.uuid4().hex
                    if request_id in self.messages_awaiting_response[message_type].get(peername, {}):
                        self.logger.warning(f'Request id {request_id} for type {message_type} for peer {peername} already in self.messages_awaiting_response, ignoring')
                        send_to_queue = False
                    else:
                        if not send_to_queue:
                            if self.disk_persistence or self.ram_persistence:
                                self.logger.warning(f'{self.source_id} Connector is currently disconnected. Response to request_id {request_id} will be sent when peer goes up, since persistence is enabled')
                                await_response = True                        
                            else:
                                self.logger.warning(f'{self.source_id} Connector is currently disconnected. Response to request_id {request_id} will not be sent since persistence is disabled')
                        else:
                            if len(self.messages_awaiting_response[message_type].get(peername, {})) > self.MAX_NUMBER_OF_AWAITING_REQUESTS:
                                self.logger.error(f'{self.source_id} messages_awaiting_response dict has {len(self.messages_awaiting_response[message_type].get(peername,{}))} entries for message type {message_type} and peername {peername}. Stop adding entries until it goes below {self.MAX_NUMBER_OF_AWAITING_REQUESTS}')
                            else:
                                self.logger.info(f'{self.source_id} Adding request {request_id} to messages_awaiting_response dict for type {message_type} and peer {peername}')
                                await_response = True
                        if await_response:
                            if peername not in self.messages_awaiting_response[message_type]:
                                self.messages_awaiting_response[message_type][peername] = {}
                            self.messages_awaiting_response[message_type][peername][request_id] = [asyncio.Event(),None]    #2nd element will contain the response set by handle_incoming_connection
                            
                if send_to_queue:            
                    queue_send = self.queue_send[peername]
                    self.logger.debug(f'{self.source_id} Putting message from queue_send_to_connector_put to queue of {peername}')
                    try:
                        if self.debug_msg_counts:
                            self.msg_counts['send_no_persist']+=1
                        queue_send.put_nowait(message_tuple)
                    except Exception:
                        self.logger.exception('queue_send_put')
                        
                if await_response:
                    try:
                        await asyncio.wait_for(self.messages_awaiting_response[message_type][peername][request_id][0].wait(), timeout=self.AWAIT_RESPONSE_TIMEOUT)
                    except asyncio.TimeoutError:
                        self.logger.warning(f'{self.source_id} Request id {request_id} timed out awaiting response')
                    else:
                        transport_json, data, binary = self.messages_awaiting_response[message_type][peername][request_id][1]
                        self.logger.info(f'{self.source_id} Receiving response and removing request {request_id} from messages_awaiting_response dict for type {message_type} and peer {peername}')
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
                await asyncio.sleep(0)
                #self.logger.debug(f'{self.source_id} Finished queue_send_to_connector_put task.')
                if not self.uds_path_send_preserve_socket:
                    return
        except asyncio.CancelledError:
            raise
        except asyncio.IncompleteReadError:
            if not await_response:
                self.logger.warning(f'{self.source_id} queue_send_to_connector_put IncompleteReadError')
        except ConnectionResetError:
            self.logger.warning(f'{self.source_id} queue_send_to_connector_put ConnectionResetError')            
        except Exception:
            self.logger.exception(f'{self.source_id} queue_send_to_connector_put')
            try:
                writer.close()
            except Exception:
                pass
      
    def store_message_to_persistence_recv(self, msg_type, message, ignore_count=False):
        persistence_path = self.persistence_recv_path + msg_type
        try:
            if os.path.exists(persistence_path) and os.path.getsize(persistence_path) > self.max_size_persistence_path:
                self.logger.warning(f'{self.source_id} Cannot store message to persistence_recv for msg_type {msg_type} because {persistence_path} is too big')
                return
            self.logger.debug(f'{self.source_id} Storing message to persistence_recv for msg_type {msg_type}')
            if DEBUG_SHOW_DATA:
                self.logger.info('With data : '+str(message[200:210]))
            with open(persistence_path, mode='ab') as fd:
                fd.write(self.PERSISTENCE_SEPARATOR)
                #fd.write(peername.encode()+b'\n')
                fd.write(message)
            if self.debug_msg_counts and not ignore_count:
                #ignore_count is nice to understand we are in a Special case. But msg_counts['load_persistence_recv'] will still contain duplicates
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
            persistence_content = None
            with open(persistence_recv_path, mode='rb') as fd:
                persistence_content = fd.read()
            persistent_count = 0
            if persistence_content:
                persistence_units = persistence_content.split(self.PERSISTENCE_SEPARATOR)[1:]
                persistence_content = None
                for message in persistence_units:
                    self.logger.debug('Loading persistent_recv message to queue : '+msg_type)
                    message_tuple = self.unpack_message(message)
                    if DEBUG_SHOW_DATA:
                        self.logger.debug('With data : '+str(message_tuple[1][:10]))
                    await dst_queue.put(message_tuple)
                    persistent_count += 1
                    #sleep(0) is important otherwise queue_recv_from_connector may have losses under high loads
                    #because of this cpu intensive loop
                    await asyncio.sleep(0)#0.001)
                    if self.debug_msg_counts:
                        self.msg_counts['load_persistence_recv']+=1                            

            self.logger.info(f'{self.source_id} load_messages_from_persistence_recv finished loading {persistent_count} messages to transition_queue. Deleting persistence file {persistence_recv_path}')
            try:
                os.remove(persistence_recv_path)
            except Exception:
                self.logger.exception(f'{self.source_id} load_messages_from_persistence_recv delete persistence file')
                            
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
                self.logger.info(f'{self.source_id} Special case : disconnection happens during transition for queue {msg_type}. Transferring {transition_queue.qsize()} messages')                
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
        #special case : if disconnection happens during the transition to connectivity, we move all content from transition queue to a new recreated file
        
        persistence_recv_enabled = []    #list of msg_types
        transition_queues = {}    #key=msg_type, value=Queue of message_bytes
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
                            msg_types_ready_for_transition.append(msg_type)
                        except Exception as exc: #ConnectionRefusedError:
                            self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to {uds_path_receive} : {exc}')
                    for msg_type in msg_types_ready_for_transition:
                        transition_queues[msg_type] = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
                        #read file into queue, delete file
                        await self.load_messages_from_persistence_recv(msg_type, transition_queues[msg_type])
                        persistence_recv_enabled.remove(msg_type)                                 
                 
                queue_recv_size = self.queue_recv.qsize()
                if queue_recv_size:
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : {queue_recv_size}')                    
                    #if queue_recv_size > 10:
                    #    self.logger.info(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : {queue_recv_size}')
                    #else:
                    #    self.logger.debug(f'{self.source_id} queue_recv_from_connector waiting with queue_recv size : {queue_recv_size}')
                else:
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector wait for data')      
                
                if transition_queues:
                    #read from transition_queues, emptying one after the other, before coming back to self.queue_recv
                    msg_type_key = list(transition_queues.keys())[0]
                    transition_queue = transition_queues[msg_type_key]                    
                    transport_json, data, binary = await transition_queue.get()
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector Received transition message with : ' + str(transport_json))
                    if transition_queue.empty():
                        self.logger.info(f'Finished reading from transition queue : {msg_type_key}')
                        del transition_queues[msg_type_key]
                else:
                    transport_json, data, binary = await self.queue_recv.get()
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector Received message with : ' + str(transport_json))
                    self.queue_recv.task_done()  #if someone uses 'join'
                
                uds_path_receive = self.uds_path_receive_from_connector.get(transport_json[MessageFields.MESSAGE_TYPE])
                if not uds_path_receive:
                    self.logger.error(f'{self.source_id} Invalid message type received by queue_recv_from_connector {transport_json[MessageFields.MESSAGE_TYPE]}')
                    transport_json, data, binary = None, None, None
                    continue

                #here we could remove some fields from transport_json if better
                message_bytes = self.pack_message(transport_json=transport_json, data=data, binary=binary)
                
                msg_type = transport_json.get(MessageFields.MESSAGE_TYPE)
                if msg_type in persistence_recv_enabled:
                    #here, if persistence_recv_enabled has a msg_type element, it means this msg_type has no connectivity, and must be kept in file                    
                    self.store_message_to_persistence_recv(msg_type, message_bytes)
                    continue
                
                if not os.path.exists(uds_path_receive):        
                    self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to non existing {uds_path_receive}')
                    await self.transition_to_persistent_recv(msg_type, message_bytes, persistence_recv_enabled, transition_queues)
                    transport_json, data, binary = None, None, None
                    continue
        
                if self.uds_path_receive_preserve_socket:
                    #this requires start_waiting_for_messages in connectors_api to have a client_connected_cb in a while loop, not a simple callback
                    use_existing_connection = False
                    #try to reuse connection to uds
                    if uds_path_receive in self.reader_writer_uds_path_receive:
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
                            self.reader_writer_uds_path_receive[uds_path_receive] = reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=uds_path_receive, 
                                                               limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                        except Exception as exc: #ConnectionRefusedError:
                            self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to {uds_path_receive} : {exc}')
                            await self.transition_to_persistent_recv(msg_type, message_bytes, persistence_recv_enabled, transition_queues)
                            transport_json, data, binary = None, None, None
                            continue                        
                        writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])                                                                
                        writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
#                       await asyncio.wait_for(writer.drain(), timeout=self.ASYNC_TIMEOUT)                                       
                        await writer.drain()                
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector finished sending data to {uds_path_receive}')                    
                    
                else:
                    #this requires start_waiting_for_messages in connectors_api to have a client_connected_cb as a simple callback, not in a while loop
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector creating new connection')
                    try:
                        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=uds_path_receive, 
                                                           limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                    except Exception as exc: #ConnectionRefusedError:
                        self.logger.warning(f'{self.source_id} queue_recv_from_connector could not connect to {uds_path_receive} : {exc}')
                        await self.transition_to_persistent_recv(msg_type, message_bytes, persistence_recv_enabled, transition_queues)                                
                        transport_json, data, binary = None, None, None
                        continue                        
                    writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])                                                                
                    writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
                    await writer.drain()                                    
                    self.logger.debug(f'{self.source_id} queue_recv_from_connector finished sending data to {uds_path_receive}')                                        
                    writer.close()                                       

                message_bytes, transport_json, data, binary = None, None, None, None                
                #await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception(f'{self.source_id} queue_recv_from_connector')
                try:
                    writer.close()
                except Exception:
                    self.logger.exception('queue_recv_from_connector writer close')
            
    async def run_server(self):
        self.logger.info('Running server')
        try:
            ssl_context = self.build_server_ssl_context() if self.use_ssl else None
            self.server = await asyncio.start_server(self.manage_full_duplex, sock=self.sock, ssl=ssl_context, limit=self.MAX_SOCKET_BUFFER_SIZE)
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
    
    async def run_client(self):
        if self.use_ssl:
            if self.ssl_allow_all:
                self.logger.info(f'{self.source_id} Running client with certificate {self.client_certificate_name}, allow all')
            else:
                self.logger.info(f'{self.source_id} Running client with certificate {self.client_certificate_name}')
        else:
            self.logger.info(f'{self.source_id} Running client without ssl')
        try:
            ssl_context = self.build_client_ssl_context() if self.use_ssl else None        
            server_hostname = '' if self.use_ssl else None
            reader, writer = await asyncio.wait_for(asyncio.open_connection(sock=self.sock, ssl=ssl_context,
                                    server_hostname=server_hostname, limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)                                                                     
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
            self.logger.info(f'{self.source_id} manage_full_duplex received new connection from client {writer.get_extra_info("peername")}')            
            self.full_duplex_connections.append(full_duplex)
        else:
            self.logger.info(f'{self.source_id} manage_full_duplex Starting connection')            
            self.full_duplex = full_duplex
        await full_duplex.start()

    def build_server_ssl_context(self):    #, client_socket=False):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        if self.ssl_allow_all:
            context.verify_mode = ssl.CERT_NONE
            context.load_cert_chain(certfile=self.ssl_helper.SERVER_PEM_PATH, keyfile=self.ssl_helper.SERVER_KEY_PATH)
            
        else:
            context.verify_mode = ssl.CERT_REQUIRED
            #OBSOLETE sni_callback : happens at each new connection. But this is necessary only for new clients connecting with newly created personal certs
            #the override enables to create a new and updated server ssl context each time a new client connects
            #this is necessary because after the new client certificate has been generated and written into SERVER_SYMLINKS_PATH, it
            #is not yet loaded automatically into the current context : so when the client reconnects to the server with its new
            #certificate, the context needs to be rebuilt in order to call again load_verify_locations(capath, which will
            #take into account the new certificate.
            #if not client_socket:
            #    context.sni_callback = self.overide_server_ssl_context
            context.load_cert_chain(certfile=self.ssl_helper.SERVER_PEM_PATH, keyfile=self.ssl_helper.SERVER_KEY_PATH)
            #for first client connection : the server ssl socket accepts new clients (using default cert)
            #and also already authenticated clients (using client certs)
            context.load_verify_locations(capath=self.ssl_helper.SERVER_SYMLINKS_PATH)
        return context
    
    #def overide_server_ssl_context(self, ssl_obj, sn, ssl_context):
    #    #rebuild ssl context by searching client cert inside capath, and using only it for this connection
    #    ssl_obj.context = self.build_server_ssl_context(client_socket=True)

    def build_client_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        if self.ssl_allow_all:
            context.verify_mode = ssl.CERT_NONE
        else:        
            context.verify_mode = ssl.CERT_REQUIRED        
            context.load_cert_chain(certfile=self.ssl_helper.CLIENT_PEM_PATH.format(self.client_certificate_name),
                                    keyfile=self.ssl_helper.CLIENT_KEY_PATH.format(self.client_certificate_name))
            #in case server certificate change, client should first replace/chain the new server certificate in its cafile
            context.load_verify_locations(cafile=self.ssl_helper.CLIENT_SERVER_CRT_PATH)    #optional
            #we might want to chain multiple certificates in CLIENT_SERVER_CRT_PATH, to support multiple server certificates
        return context


class FullDuplex:

    class TransitionClientCertificateException(Exception):
        pass
    
    def __init__(self, connector, reader, writer):
        self.connector = connector
        self.loop = self.connector.loop
        self.logger = self.connector.logger.getChild('FullDuplex')
        self.reader = reader
        self.writer = writer
        self.is_stopping = False
        #self.use_ack = self.connector.use_ack        
        #self.use_ack_is_list = isinstance(self.use_ack, list)                    
        self.transport_id = 0
        self.MAX_TRANSPORT_ID = self.connector.MAX_TRANSPORT_ID
        self.MAX_RETRIES_BEFORE_ACK = self.connector.MAX_RETRIES_BEFORE_ACK
        self.ack_dict = {}

    def stop_nowait_for_persistence(self, message_tuple=None):
        #at disconnection, this functions stores queue_send remainings into persistence
        #if exists, this is the first data to be stored into persistence
        #message_tuple if exists is the last message processed during disconnection in handle_outgoing_connection_queue,
        #and will then be the first message stored into persistence
        try:
            self.logger.info(f'{self.connector.source_id} stop_nowait_for_persistence')
            #remove queue (for queue_send_to_connector_put) and send remainings (if exist) of queue_send to persistence file
            queue_send = self.connector.queue_send.pop(self.peername, None)
            if queue_send:
                self.logger.info(f'stop_nowait_for_persistence queue_send size {queue_send.qsize()}')
                disk_persistence_is_list = isinstance(self.connector.disk_persistence, list)
                if message_tuple:
                    disk_persistence = True
                    if disk_persistence_is_list:
                        #disk_persistence can be a list of message types
                        disk_persistence = (message_tuple[0].get(MessageFields.MESSAGE_TYPE) in self.connector.disk_persistence)
                    if disk_persistence:
                        message = self.connector.pack_message(transport_json=message_tuple[0],
                                                              data=message_tuple[1], binary=message_tuple[2])
                        self.logger.info('Last message, Storing message to persistence to peername : '+self.peername)
                        self.connector.store_message_to_persistence(self.peername, message)                        
                
                count = 0
                while not queue_send.empty():
                    transport_json, data, binary = queue_send.get_nowait()
                    disk_persistence = True
                    if disk_persistence_is_list:
                        #disk_persistence can be a list of message types
                        disk_persistence = (transport_json.get(MessageFields.MESSAGE_TYPE) in self.connector.disk_persistence)
                    if disk_persistence:                    
                        count += 1
                        message = self.connector.pack_message(transport_json=transport_json,
                                                              data=data, binary=binary)
                        self.logger.info(f'Emptying queue_send, Storing message number {count} to persistence to peername {self.peername}')
                        self.connector.store_message_to_persistence(self.peername, message)                                              
        except Exception:
            self.logger.exception(f'{self.connector.source_id} stop_nowait_for_persistence')            
            
        
    def stop_task(self, client_wait_for_reconnect=False):
        self.loop.create_task(self.stop(client_wait_for_reconnect=client_wait_for_reconnect))
        
    async def stop(self, client_wait_for_reconnect=False, hard=False):
        try:
            self.is_stopping = True
            self.logger.info(f'{self.connector.source_id} stop FullDuplex to peer {self.peername}')
            self.logger.info(f'queue_recv size {self.connector.queue_recv.qsize()} , queue_send size {self.connector.queue_send[self.peername].qsize() if self.peername in self.connector.queue_send else "popped"}')            
            #first, cancel incoming task to stop receiving messages from peer into queue_recv
            self.connector.cancel_tasks(task_names=[self.peername+'_incoming'])
            #we never stop listening to uds queue_send_to_connector, but we remove peername from queue_send so that it stops writing messages into queue_send
            #this has been done already in stop_nowait_for_persistence in case of persistence, but need to be done here otherwise
            if not self.connector.disk_persistence:            
                #this has been done already in stop_nowait_for_persistence if disk_persistence
                self.logger.info(f'{self.connector.source_id} stop, now deleting queue_send of peer {self.peername}')
                queue_send = self.connector.queue_send.pop(self.peername, None)
                if queue_send:
                    if hard:
                        self.logger.info(self.connector.source_id+' not waiting for queue_send to empty (hard stop), with peer '+self.peername)                                
                    else:
                        self.logger.info(self.connector.source_id+' waiting for queue_send to empty (soft stop), with peer '+self.peername)
                        #let handle_outgoing_connection send remainings of queue_send into peer                        
                        await queue_send.join()

            if hard:
                self.logger.info(self.connector.source_id+' not waiting for queue_recv to empty (hard stop), with peer '+self.peername)                                
            else:
                self.logger.info(self.connector.source_id+' waiting for queue_recv to empty (soft stop), with peer '+self.peername)
                #let queue_recv_from_connector read all messages from queue_recv                
                await self.connector.queue_recv.join()

            if hard:
                self.logger.info(self.connector.source_id+' stop, without waiting for queues to peer '+self.peername)    
            else:
                self.logger.info(self.connector.source_id+' stop, after waiting for queues to peer '+self.peername)    
            self.logger.info(f'queue_recv size {self.connector.queue_recv.qsize()} , queue_send size {self.connector.queue_send[self.peername].qsize() if self.peername in self.connector.queue_send else "popped"}')            
            
            self.writer.close()
            if PYTHON_GREATER_37:
                try:
                    await self.writer.wait_closed()      #python 3.7                                              
                except Exception as exc:
                    #cath exception because of scenario seen in TEST_PERSISTENCE_CLIENT with TEST_WITH_SSL=False
                    self.logger.warning(self.connector.source_id+' stop wait_closed : '+str(exc))
                    
            self.loop.create_task(self.connector.cancel_tasks_async(task_names=[self.peername+'_incoming', self.peername+'_outgoing']))        
            
            if client_wait_for_reconnect and not self.connector.is_server:
                #try to restart the client if server disconnected
                self.connector.tasks['client_wait_for_reconnect'] = self.loop.create_task(self.connector.client_wait_for_reconnect())    
            
        except Exception as exc:
            self.logger.exception(self.connector.source_id+' stop : '+str(exc))
            
    async def start(self):
        try:
            self.logger.info(f'{self.connector.source_id} start FullDuplex')
            self.is_stopping = False            
            peer_identification_finished = False
            if self.connector.is_server:
                if self.connector.use_ssl:
                    if not self.connector.ssl_allow_all:
                        peer_cert = self.writer.get_extra_info('ssl_object').getpeercert()
    # {'subject': ((('organizationName', '{}'),), (('commonName', '{}'),)), 'issuer': ((('organizationName', '{}'),), (('commonName', '{}'),)), 'version': 1, 'serialNumber': '8F7A25089D8D4DF0F3FE6CE5B1DA059C7D6837', 'notBefore': 'Feb 25 10:20:26 2020 GMT', 'notAfter': 'Mar 26 10:20:26 2020 GMT'}
                        #for client peer validation
                        #client_certificate_common_name = peer_cert["subject"][1][0][1]
                        
                        self.client_certificate_serial = peer_cert['serialNumber']
    
                        if self.client_certificate_serial != self.connector.ssl_helper.default_client_serial:
                            peername = self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(self.client_certificate_serial)
                            if not peername:
                                self.logger.error(f'Authorized client with certificate {self.client_certificate_serial} has no source_id ! Aborting')
                                raise Exception('Unknown client')
                            peer_identification_finished = True                    
                        else:
                            #we could use the default_client_serial, but prefer to have a unique peername per client
                            #for rare case where 2 clients are connecting simultaneously and have same default_client_serial
                            peername = str(self.writer.get_extra_info('peername'))
                            #for client with default certificate, creation of handle_outgoing_connection task is not necessary and not performed
                    else:
                        #in ssl_allow_all mode, no cert can be obtained, peer_identification_finished will be finished in handle_ssl_messages_server
                        peername = str(self.writer.get_extra_info('peername'))
                else:
                    #this creates a temporary entry in queue_send, peername will be replaced by client_id after handshake_no_ssl
                    peername = str(self.writer.get_extra_info('peername'))     
                    #for server without ssl, creation of handle_outgoing_connection task will be done in handle_handshake_no_ssl_server
            else:
                peername = str(self.connector.server_sockaddr)
                peer_identification_finished = True
                
            if peername+'_incoming' in self.connector.tasks:
                #problem here in case of server, after client without ssh had its peername replaced. we won't detect the redundant connection
                self.logger.warning('peername : '+str(peername)+' already connected : Disconnecting and reconnecting...')
                self.connector.cancel_tasks(task_names=[peername+'_incoming', peername+'_outgoing'])
                
            self.peername = peername

            if self.connector.is_server and self.connector.hook_server_auth_client:
                accept = await self.connector.hook_server_auth_client(self.peername)
                if accept:
                    self.logger.info(f'{self.connector.source_id} accepting client {self.peername}')
                else:
                    self.logger.info(f'{self.connector.source_id} blocking client {self.peername}')
                    await self.stop()
                    return

            if peer_identification_finished:
                self.logger.info(f'{self.connector.source_id} start FullDuplex peer_identification_finished for {self.peername}')
            else:
                self.logger.info(f'{self.connector.source_id} start FullDuplex peer identification not finished yet for {self.peername}')
                                        
            if self.peername not in self.connector.queue_send:
                self.logger.info(self.connector.source_id+' Creating queue_send for peername : '+str(self.peername))
                self.connector.queue_send[self.peername] = asyncio.Queue(maxsize=self.connector.MAX_QUEUE_SIZE)            
                
            #self.lock_connection = asyncio.Lock()    #to not mix send and recv internal steps
            task_incoming_connection = self.loop.create_task(self.handle_incoming_connection())
            self.connector.tasks[self.peername+'_incoming'] = task_incoming_connection
            
            if peer_identification_finished:
                task_outgoing_connection = self.loop.create_task(self.handle_outgoing_connection())
                self.connector.tasks[self.peername+'_outgoing'] = task_outgoing_connection
                
            self.logger.info(f'{self.connector.source_id} start FullDuplex, Now tasks are : '+str(list(self.connector.tasks.keys())))
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception(f'{self.connector.source_id} start FullDuplex')            
            if self.connector.disk_persistence:
                self.stop_nowait_for_persistence()
            await self.stop()            
            
    def server_validates_client(self, transport_json):
        #called only if self.connector.use_ssl:
        #check if client_certificate_serial in peer client certificate is the serial of the certificate created by server for the requested source_id
        if (not self.connector.ssl_allow_all) and ( self.client_certificate_serial != self.connector.ssl_helper.source_id_2_cert['source_id_2_cert'].get(transport_json[MessageFields.SOURCE_ID]) ):
            self.logger.warning('Client {} tried to impersonate client {}'.format(self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(self.client_certificate_serial), transport_json[MessageFields.SOURCE_ID]))
            return False
        return True
        
    async def handle_incoming_connection(self):
        self.logger.info(f'{self.connector.source_id} Starting handle_incoming_connection with peer {self.peername}')
        if not self.connector.is_server:   
            if self.connector.use_ssl:
                try:
                    #if no unique client_certificate yet, client starts by sending get_new_certificate
                    await self.handle_ssl_messages_client()
                except self.TransitionClientCertificateException:
                    self.logger.info(self.connector.source_id+' Client transitioning to unique certificate')
                    self.loop.create_task(self.connector.restart(connector_socket_only=True))#False))
                    return
                except Exception:    
                    self.logger.exception(self.connector.source_id+' handle_incoming_connection')
                    return
            else:
                try:
                    #client sends hello
                    await self.handle_handshake_no_ssl_client()
                except Exception:
                    self.logger.exception(self.connector.source_id+' handle_incoming_connection')                    
                    return                   
        
        while True:
            try:            
                self.logger.debug(self.connector.source_id+' handle_incoming_connection waiting for message')
                transport_json, data, binary = await self.recv_message()
                message_type = transport_json.get(MessageFields.MESSAGE_TYPE)                
                if self.connector.is_server:
                    if self.connector.use_ssl:
                        if message_type == '_ssl':
                            #server waits for get_new_certificate
                            await self.handle_ssl_messages_server(data, transport_json)
                            #don't send ssl messages to queues
                            return
                            valid_client = self.server_validates_client(transport_json)
                            if self.connector.validates_clients:
                                if not valid_client:
                                    self.stop_task()
                                    return
                        elif message_type == '_handshake_ssl':
                            #server waits for _handshake_ssl from client                        
                            await self.handle_ssl_messages_server(data, transport_json)  
                            #don't send _handshake_ssl messages to queues
                            continue                        
                            
                    else:
                        if message_type == '_handshake_no_ssl':      
                            #server waits for handshake_no_ssl from client                        
                            await self.handle_handshake_no_ssl_server(data, transport_json)  
                            #don't send handshake_no_ssl messages to queues
                            continue                        
                self.logger.debug(self.connector.source_id+' handle_incoming_connection received from peer : ' + json.dumps(transport_json))
                if DEBUG_SHOW_DATA:
                    self.logger.info('handle_incoming_connection received data from peer : ' + str(data))
                    if binary:
                        self.logger.info('handle_incoming_connection received binary from peer : ' + str(binary))                    
                
                if message_type == '_ack':     
                    #if ACK is received, update accordingly the ack_dict
                    #this could be checked only when use_ack is true, because for it to happen we must have sent transport_id beforehand                    
                    transport_id = transport_json.pop(MessageFields.TRANSPORT_ID, None)                                        
                    if transport_id and (transport_id in self.ack_dict):
                        self.ack_dict[transport_id].set()
                    else:
                        self.logger.warning(f'handle_incoming_connection ACK received from peer {self.peername} with unknown transport_id {transport_id}. Ignoring...')
                    continue
                elif message_type == '_ping':
                    #if _ping is received with await_response, it is a ping request : reply immediately WITHOUT await_response, to prevent ping infinite loop.
                    #if _ping is received without await_response, it is a ping reply, that we forward as usual
                    if transport_json.get(MessageFields.AWAIT_RESPONSE):
                        transport_json_ping_reply = {MessageFields.MESSAGE_TYPE:'_ping', MessageFields.RESPONSE_ID:transport_json[MessageFields.REQUEST_ID]}                        
                        await self.send_message(transport_json=transport_json_ping_reply, data=data, binary=binary)
                        transport_json, data, binary = None, None, None
                        continue
                
                #at this stage, the message received is never an ACK                
                transport_id = transport_json.pop(MessageFields.TRANSPORT_ID, None)                    
                if transport_id:
                    #if peer sent a transport_id, we must answer ACK
                    #we could have done this only if use_ack is true, but now it means that we are not dependent on configuration to support peers having use_ack true
                    self.logger.info(f'handle_incoming_connection {self.connector.source_id} Sending ACK to peer {self.peername} for transport id {transport_id}')
                    transport_json_ack = {MessageFields.TRANSPORT_ID:transport_id, MessageFields.MESSAGE_TYPE:'_ack'}
                    await self.send_message(transport_json=transport_json_ack)                                           
                
                if self.connector.ignore_peer_traffic:
                    ignore_peer_traffic = False
                    if self.connector.ignore_peer_traffic is True:
                        ignore_peer_traffic = True
                    else:
                        ignore_peer_traffic = (self.peername in self.connector.ignore_peer_traffic)
                    if ignore_peer_traffic:
                        self.logger.debug(f'{self.connector.source_id} handle_incoming_connection : Ignoring message from peer {self.peername}')
                        transport_json, data, binary = None, None, None
                        continue
                    
                if (message_type not in self.connector.recv_message_types) and (message_type != '_ping'):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} received a message with invalid type {message_type}. Ignoring...')   
                    put_msg_to_queue_recv = False
                else:
                    put_msg_to_queue_recv = True
                    
                if put_msg_to_queue_recv:
                    #dump file if with_file before sending message_tuple to queue
                    with_file = transport_json.get(MessageFields.WITH_FILE)
                    #dict {'src_path':, 'dst_name':, 'dst_type':, 'binary_offset':}
                    if with_file:
                        dst_dirpath = self.connector.file_type2dirpath.get(with_file.get('dst_type'))
                        if dst_dirpath:
                            try:
                                if not binary:
                                    binary = b''
                                binary_offset = with_file.get('binary_offset', 0)                            
                                dst_fullpath = os.path.join(dst_dirpath, with_file.get('dst_name',''))
                                if os.path.exists(dst_fullpath):
                                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} trying to override existing file {dst_fullpath}, ignoring...')
                                else:
                                    self.logger.info(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} writing received file to {dst_fullpath}')
                                    with open(dst_fullpath, 'wb') as fd:
                                        fd.write(binary[binary_offset:])
                                #remove file from binary, whether having written it to dst_fullpath or not. To prevent bloating
                                binary = binary[:binary_offset]
                                if len(binary) == 0:
                                    if MessageFields.WITH_BINARY in transport_json:
                                        del transport_json[MessageFields.WITH_BINARY]
                            except Exception:
                                self.logger.exception(f'{self.connector.source_id} from peer {self.peername} handle_incoming_connection with_file')
                        else:
                            self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} tried to create file in non existing directory {dst_dirpath} for type {with_file.get("dst_type")}, ignoring...')
                    
                    #check if this message is a response to an awaiting request, and update put_msg_to_queue_recv
                    response_id = transport_json.get(MessageFields.RESPONSE_ID)                                
                    if response_id:
                        if response_id not in self.connector.messages_awaiting_response[message_type].get(self.peername, {}):
                            self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} got response_id {response_id} not existing in messages_awaiting_response for type {message_type}. Forwarding to queue_recv anyway...')
                        else:
                            #set the response in case this is the response to a request that came with AWAIT_RESPONSE true
                            self.logger.debug(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} got response_id {response_id} in messages_awaiting_response for type {message_type}')                            
                            self.connector.messages_awaiting_response[message_type][self.peername][response_id][1] = (transport_json, data, binary)
                            self.connector.messages_awaiting_response[message_type][self.peername][response_id][0].set()
                            put_msg_to_queue_recv = False
                            
                if put_msg_to_queue_recv:                   
                    # send the message to queue
                    self.logger.debug(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} putting message to queue_recv')                    
                    self.connector.queue_recv.put_nowait((transport_json, data, binary))

                transport_json, data, binary = None, None, None
            except asyncio.CancelledError:
                raise     
            except Exception as exc:
                if isinstance(exc, asyncio.IncompleteReadError):                
                    if self.connector.is_server:                
                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} Client disconnected')
                    else:                    
                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} Server disconnected')
                elif isinstance(exc, ConnectionResetError):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} ConnectionResetError : {exc}')
                elif isinstance(exc, ssl.SSLError):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername} SSLError : {exc}')
                else:
                    self.logger.exception(f'{self.connector.source_id} handle_incoming_connection from peer {self.peername}')
                if not self.is_stopping:
                    if self.connector.disk_persistence:                    
                        self.stop_nowait_for_persistence()      
                    self.stop_task(client_wait_for_reconnect=True)
                return
           
    async def handle_outgoing_connection(self):
        if self.connector.disk_persistence:
            if self.peername not in self.connector.queue_send_transition_to_connect:
                if self.connector.persistence_existence_check(self.peername):
                    self.logger.info(f'{self.connector.source_id} Creating queue_send_transition_to_connect for peer {self.peername}')
                    self.connector.queue_send_transition_to_connect[self.peername] = asyncio.Queue(maxsize=self.connector.MAX_QUEUE_SIZE)                
        
        if self.peername in self.connector.queue_send_transition_to_connect:
            #loading persistence messages into queue_send_transition_to_connect
            await self.connector.load_messages_from_persistence(self.peername)            
            queue_send = self.connector.queue_send_transition_to_connect[self.peername]        
            self.logger.info(f'{self.connector.source_id} Entering handle_outgoing_connection_queue for peer {self.peername} with queue_send_transition_to_connect of length {queue_send.qsize()}')
            await self.handle_outgoing_connection_queue(queue_send, lambda :not queue_send.empty())
            del self.connector.queue_send_transition_to_connect[self.peername]
            
        queue_send = self.connector.queue_send[self.peername]
        self.logger.info(f'{self.connector.source_id} Entering handle_outgoing_connection_queue for peer {self.peername} with queue_send of length '+str(queue_send.qsize()))        
        await self.handle_outgoing_connection_queue(queue_send, lambda :True)
        
    async def handle_outgoing_connection_queue(self, queue_send, condition): 
        while condition():
            try:
                self.logger.debug(self.connector.source_id+' handle_outgoing_connection wait for queue_send')        
                transport_json, data, binary = message_tuple = await queue_send.get()
                #self.connector.msg_counts['queue_sent']+=1
                self.logger.debug(self.connector.source_id+' Received message from queue_send : ' + str(transport_json))
                if DEBUG_SHOW_DATA:
                    self.logger.info('With data : '+str(data))
                queue_send.task_done()  #if someone uses 'join'    

                with_file_dict = transport_json.get(MessageFields.WITH_FILE)                
                #embed file if WITH_FILE
                file_src_path = None
                if with_file_dict:
                    file_src_path = str(with_file_dict['src_path'])#, with_file_dict['dst_name'], with_file_dict['dst_type'], with_file_dict['binary_offset']=0
                    binary_file = None
                    try:
                        with open(file_src_path, 'rb') as fd:
                            binary_file = fd.read()
                    except Exception:
                        self.logger.exception('handle_outgoing_connection handling file : '+str(file_src_path))
                        del transport_json[MessageFields.WITH_FILE]
                    else:
                        if binary_file:
                            #append the file byte content to "binary"
                            if binary:
                                len_binary = len(binary)
                                self.logger.info('handle_outgoing_connection prepare message with both binary and binary file at offset '+str(len_binary))
                                with_file_dict['binary_offset'] = len_binary
                                binary = binary + binary_file
                            else:
                                binary = binary_file
                                transport_json[MessageFields.WITH_BINARY] = True
                            binary_file = None

                #use_ack = self.use_ack
                #if self.use_ack_is_list:
                #    use_ack = (transport_json[MessageFields.MESSAGE_TYPE] in self.connector.use_ack)
                #if use_ack or transport_json.get(MessageFields.WAIT_FOR_ACK):
                if transport_json.get(MessageFields.WAIT_FOR_ACK):                    
                    #if this message has wait_for_ack true, we add the transport_id field to tell the peer that ACK is expected
                    #then we send message, and wait for ACK from peer before going on (ACK is expected in handle_incoming_connection)
                    self.transport_id += 1
                    if self.transport_id > self.MAX_TRANSPORT_ID:
                        self.transport_id = 0
                    self.ack_dict[self.transport_id] = asyncio.Event()
                    transport_json[MessageFields.TRANSPORT_ID] = self.transport_id
                    retry = 0
                    while retry <= self.MAX_RETRIES_BEFORE_ACK:
                        self.logger.debug(f'handle_outgoing_connection {self.connector.source_id} send message with transport_id {self.transport_id} expecting ACK')
                        await self.send_message(transport_json=transport_json, data=data, binary=binary)
                        transport_json, data, binary, message_tuple = None, None, None, None
                        
                        try:
                            await asyncio.wait_for(self.ack_dict[self.transport_id].wait(), timeout=self.connector.ASYNC_TIMEOUT)
                            self.logger.info(f'handle_outgoing_connection {self.connector.source_id} received ACK for transport id {self.transport_id}')
                            del self.ack_dict[self.transport_id]
                            break
                        except asyncio.TimeoutError:
                            self.logger.warning(f'handle_outgoing_connection timed out waiting for ACK for transport id {self.transport_id} at retry {retry}')
                            retry += 1
                    else:
                        msg = f'handle_outgoing_connection ACK was not received for transport id {self.transport_id}'
                        self.logger.warning(msg)
                        #do we want to just go on, or restart ?
                        #raise Exception(msg)
                else:
                    #send the message
                    self.logger.debug(f'handle_outgoing_connection sending message')
                    await self.send_message(transport_json=transport_json, data=data, binary=binary)
                    transport_json, data, binary, message_tuple = None, None, None, None
                    
                
                if file_src_path:
                    if with_file_dict.get('delete',True):
                        #delete file by default, unless specified False
                        try:
                            self.logger.info(f'handle_outgoing_connection Removing file {file_src_path} after upload')
                            os.remove(file_src_path)
                        except Exception:
                            self.logger.exception(f'handle_outgoing_connection trying to remove file {file_src_path}')
                                
            except asyncio.CancelledError:
                raise                                
            except Exception:
                self.logger.exception(self.connector.source_id+' handle_outgoing_connection')
                if not self.is_stopping:                
                    if self.connector.disk_persistence:       
                        if queue_send != self.connector.queue_send.pop(self.peername, None):
                            self.logger.info(f'Special case : disconnection happens during transition. Transferring {queue_send.qsize()} messages')
                            #we should copy queue_send_transition_to_connect content into a new recreated persistent file                            
                            count = 0
                            disk_persistence_is_list = isinstance(self.connector.disk_persistence, list)
                            while not queue_send.empty():
                                transport_json, data, binary = queue_send.get_nowait()
                                disk_persistence = True                                
                                if disk_persistence_is_list:
                                    #disk_persistence can be a list of message types
                                    disk_persistence = (transport_json.get(MessageFields.MESSAGE_TYPE) in self.connector.disk_persistence)
                                if disk_persistence:                    
                                    count += 1
                                    message = self.connector.pack_message(transport_json=transport_json,
                                                                          data=data, binary=binary)
                                    self.logger.info(f'Emptying transition queue_send, Storing message number {count} to persistence to peername {self.peername}')
                                    self.connector.store_message_to_persistence(self.peername, message, ignore_count=True)                                              
                        else:
                            #regular case of disconnection
                            self.stop_nowait_for_persistence(message_tuple=message_tuple)
                    self.stop_task(client_wait_for_reconnect=True)                    
                return


    #4|2|json|4|data|4|binary
    async def send_message(self, message=None, message_type=None, source_id=None, destination_id=None, request_id=None, response_id=None,
                     transport_json=None, data=None, binary=None):
        try:
#            async with self.lock_connection:       
            update_msg_counts = True                   
            if message:
                self.logger.debug(self.connector.source_id+' send_message of length {}'.format(len(message)))                         
                if DEBUG_SHOW_DATA:
                    self.logger.info('and with data {}'.format(message))                                                  
            else:
                if transport_json:
                    self.logger.debug(f'{self.connector.source_id} send_message {message_type or transport_json} with data length {len(data or "")}')     
                    if DEBUG_SHOW_DATA:
                        self.logger.info('and with data {}'.format(data))
                        if binary:
                            self.logger.info('and with binary {}'.format(binary))

                    #fill source_id (which is mandatory) if not supplied                        
                    if MessageFields.SOURCE_ID not in transport_json:
                        transport_json[MessageFields.SOURCE_ID] = self.connector.source_id
                else:
                    #internal cases only like ssl/handhake_no_ssl, ack
                    if not source_id:
                        source_id = self.connector.source_id
                message = self.connector.pack_message(message_type=message_type, source_id=source_id, destination_id=destination_id, 
                                                      request_id=request_id, response_id=response_id, transport_json=transport_json,
                                                      data=data, binary=binary)
                if DEBUG_SHOW_DATA:
                    self.logger.info('send_message full message ready to send : '+str(message))    

                if transport_json and transport_json[MessageFields.MESSAGE_TYPE] == '_ack':
                    update_msg_counts = False
            # send the length to be sent next
            self.writer.write(message[:Structures.MSG_4_STRUCT.size])
            self.writer.write(message[Structures.MSG_4_STRUCT.size:])        
            try:
                await asyncio.wait_for(self.writer.drain(), timeout=self.connector.ASYNC_TIMEOUT)
            except asyncio.TimeoutError as exc:
                self.logger.warning('send_message TimeoutError : '+str(exc))
                
            if self.connector.debug_msg_counts and update_msg_counts:
                self.connector.msg_counts['queue_sent']+=1
            
            self.logger.debug(self.connector.source_id+' send_message Finished sending message')
        except asyncio.CancelledError:
            raise            
        except ConnectionResetError as exc:
            self.logger.warning(self.connector.source_id+' ConnectionResetError : '+str(exc)+' with peer '+self.peername)
            raise        
        except Exception:
            self.logger.exception(self.connector.source_id+' send_message with peer '+self.peername)
            
            
    #4|2|json|4|data|4|binary
    async def recv_message(self):
        try:
            self.logger.debug(self.connector.source_id+ ' recv_message')
            next_length_bytes = await self.reader.readexactly(Structures.MSG_4_STRUCT.size)
            #async with self.lock_connection:      
            message_size = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
            self.logger.debug('recv_message got message of length : '+str(message_size))            
            message = await self.reader.readexactly(message_size)               
            #message = await asyncio.wait_for(self.reader.readexactly(message_size), timeout=self.connector.ASYNC_TIMEOUT)
            transport_json, data, binary = self.connector.unpack_message(next_length_bytes+message)
            if self.connector.debug_msg_counts:
                if transport_json.get(MessageFields.MESSAGE_TYPE) != '_ack':
                    self.connector.msg_counts['queue_recv']+=1
            
            self.logger.debug('recv_message with : '+str(transport_json)+', and data length : '+str(len(data)))
            if DEBUG_SHOW_DATA:                
                self.logger.info('and with data {}'.format(data))
                if binary:
                    self.logger.info('and with binary {}'.format(binary))
                
            return [transport_json, data, binary]                

        except asyncio.CancelledError:
            raise            
        except asyncio.IncompleteReadError:
            self.logger.warning(f'{self.connector.source_id} recv_message : peer {self.peername} disconnected')
            raise
        except ConnectionResetError as exc:
            self.logger.warning(f'{self.connector.source_id} recv_message ConnectionResetError : {exc} with peer {self.peername}')
            raise
        except ssl.SSLError as exc:
            self.logger.warning(f'{self.connector.source_id} recv_message SSLError : {exc} with peer {self.peername}')
            raise
        except Exception:
            self.logger.exception(f'{self.connector.source_id} recv_message with peer {self.peername}')
            raise
        
    async def handle_ssl_messages_server(self, data=None, transport_json=None):
        try:
            if not self.connector.ssl_allow_all:
                data_json = json.loads(data.decode())                
                if data_json.get('cmd') == 'get_new_certificate':
                    
                    if self.client_certificate_serial != self.connector.ssl_helper.default_client_serial: 
                        self.logger.warning('handle_ssl_messages_server Client {} tried to get_new_certificate with private certificate. Stopping...'.format(self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(self.client_certificate_serial)))
                        self.stop_task() 
                        return
                    
                    self.logger.info('handle_ssl_messages_server receiving get_new_certificate, and calling create_client_certificate')                    
                    #we could have check if client current certificate is default, but is seems limiting, code would be like :
                    #cert_der = self.writer.get_extra_info("ssl_object").getpeercert()
                    #common_name = cert_der["subject"][1][0][1]
                    #if common_name == ssl.DEFAULT_CLIENT_CERTIFICATE_COMMON_NAME:                         
                    crt_path, key_path = await self.connector.ssl_helper.create_client_certificate(source_id=transport_json[MessageFields.SOURCE_ID], common_name=None)
                    with open(crt_path, 'r') as fd:
                        crt = fd.read()
                    with open(key_path, 'r') as fd:
                        key = fd.read()                        
                    response = {'cmd': 'set_new_certificate',
                                'crt': crt,
                                'key': key}
                    #we might want to delete now the client private key from server :
                    if self.connector.DELETE_CLIENT_PRIVATE_KEY_ON_SERVER:
                        os.remove(key_path)
                    params_as_string = json.dumps(response)
                    self.logger.info('handle_ssl_messages_server sending set_new_certificate')                
                    await self.send_message(message_type='_ssl', data=params_as_string)
                else:
                    self.logger.warning('handle_ssl_messages_server got invalid command : '+str(data_json.get('cmd')))
                #now server disconnects
                self.stop_task()
            else:
                if data != b'hello':            
                    self.logger.warning(f'Received bad handshake ssl data : {data[:100]}, from client : {transport_json[MessageFields.SOURCE_ID]}')
                    self.stop_task()
                    return          
                self.logger.info('Received handshake ssl from client : {}'.format(transport_json[MessageFields.SOURCE_ID]))
                old_peername = self.peername
                new_peername = transport_json[MessageFields.SOURCE_ID]
                self.logger.info('Replacing peername {} by {}'.format(old_peername, new_peername))
                self.peername = new_peername                
                self.connector.queue_send[new_peername] = self.connector.queue_send.pop(old_peername)
                self.connector.tasks[self.peername+'_incoming'] = self.connector.tasks.pop(old_peername+'_incoming')
                task_outgoing_connection = self.loop.create_task(self.handle_outgoing_connection())
                self.connector.tasks[self.peername+'_outgoing'] = task_outgoing_connection
                    
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception('handle_ssl_messages_server')
            self.stop_task()
                    
    async def handle_ssl_messages_client(self):
        try:
            if not self.connector.ssl_allow_all:
                if self.connector.client_certificate_name == self.connector.ssl_helper.CLIENT_DEFAULT_CERT_NAME:
                    params_as_string = json.dumps({'cmd':'get_new_certificate'})    #, 'source_id':self.connector.source_id})
                    self.logger.info('handle_ssl_messages_client sending get_new_certificate')                
                    await self.send_message(message_type='_ssl', data=params_as_string)
                    
                    transport_json, data, binary = await self.recv_message()
                    if transport_json[MessageFields.MESSAGE_TYPE] != '_ssl':
                        msg = 'handle_ssl_messages_client received bad message_type : '+str(transport_json)
                        self.logger.warning(msg)
                        raise Exception(msg)                     
    
                    data_json = json.loads(data.decode())
                    if data_json.get('cmd') == 'set_new_certificate':
                        self.logger.info('handle_ssl_messages_client receiving set_new_certificate')                                          
                        crt, key = data_json.get('crt'), data_json.get('key')
                        with open(self.connector.ssl_helper.CLIENT_PEM_PATH.format(self.connector.source_id), 'w') as fd:
                            fd.write(crt)
                        with open(self.connector.ssl_helper.CLIENT_KEY_PATH.format(self.connector.source_id), 'w') as fd:
                            fd.write(key)
                        #close this connection, and open new connection with newly received certificate
                        self.connector.client_certificate_name = self.connector.source_id
                        #self.stop_task()
                        raise self.TransitionClientCertificateException()
                        
                    else:
                        msg = 'handle_ssl_messages_client got invalid command : '+str(data_json.get('cmd'))
                        self.logger.warning(msg)
                        raise Exception(msg)                     
            else:
                self.logger.info('handle_ssl_messages_client sending hello')            
                await self.send_message(message_type='_handshake_ssl', data='hello')
                
        except self.TransitionClientCertificateException:
            #restart client connector with newly received certificate
            raise
        except asyncio.IncompleteReadError:
            self.logger.warning('Server disconnected')
            self.stop_task(client_wait_for_reconnect=True)            
            raise    
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception('handle_ssl_messages_client')
            self.stop_task(client_wait_for_reconnect=True)            
            raise
                    
    async def handle_handshake_no_ssl_server(self, data=None, transport_json=None):
        try:
            if data != b'hello':
                self.logger.warning(f'Received bad handshake_no_ssl data : {data[:100]}, from client : {transport_json[MessageFields.SOURCE_ID]}')
                self.stop_task()
                return          
            self.logger.info('Received handshake_no_ssl from client : {}'.format(transport_json[MessageFields.SOURCE_ID]))
            old_peername = self.peername    #str(self.writer.get_extra_info('peername'))
            new_peername = transport_json[MessageFields.SOURCE_ID]
            self.logger.info('Replacing peername {} by {}'.format(old_peername, new_peername))
            self.peername = new_peername
            #self.logger.info('yomo self.connector.tasks : '+str(self.connector.tasks))
            #self.connector.tasks[new_peername+'_outgoing'] = self.connector.tasks.pop(old_peername+'_outgoing')
            
            self.connector.queue_send[new_peername] = self.connector.queue_send.pop(old_peername)
            self.connector.tasks[self.peername+'_incoming'] = self.connector.tasks.pop(old_peername+'_incoming')
            #if old_peername in self.connector.queue_send_transition_to_connect:
            #    self.connector.queue_send_transition_to_connect[new_peername] = self.connector.queue_send_transition_to_connect.pop(old_peername)
            
            #now we can create handle_outgoing_connection, where queue_send_transition_to_connect will be updated with new_peername
            task_outgoing_connection = self.loop.create_task(self.handle_outgoing_connection())
            self.connector.tasks[self.peername+'_outgoing'] = task_outgoing_connection
            
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception('handle_handshake_no_ssl_server')
            self.stop_task()
            raise
        
    async def handle_handshake_no_ssl_client(self):
        try:
            #the purpose of send_message is to send self.connector.source_id in transport_json
            self.logger.info('handle_handshake_no_ssl_client sending hello')            
            await self.send_message(message_type='_handshake_no_ssl', data='hello')
                
        except asyncio.IncompleteReadError:
            self.logger.warning('handle_handshake_no_ssl_client Server disconnected')
            self.stop_task(client_wait_for_reconnect=True)            
            raise    
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception('handle_handshake_no_ssl_client')
            self.stop_task(client_wait_for_reconnect=True)            
            raise
            
