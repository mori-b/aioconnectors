import os
import asyncio
import json
from functools import partial
import uuid

from .helpers import get_logger, chown_nobody_permissions
from .core import Connector
from .connection import Structures

DEFAULT_LOGGER_NAME = 'aioconnectors'
LOGFILE_DEFAULT_NAME = 'aioconnectors.log'
DEFAULT_LOGGER_LOG_LEVEL = 'INFO'

class ConnectorManager:
    def __init__(self, config_file_path=None, logger=None, use_default_logger=True, 
                 default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL, default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH,
                 is_server=True, server_sockaddr=None, use_ssl=Connector.USE_SSL, ssl_allow_all=False, 
                 client_bind_ip=None, certificates_directory_path=None, client_name=None, 
                 send_message_types=None, recv_message_types=None, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH,
                 disk_persistence_send=Connector.DISK_PERSISTENCE_SEND, 
                 disk_persistence_recv=Connector.DISK_PERSISTENCE_RECV, 
                 max_size_persistence_path=Connector.MAX_SIZE_PERSISTENCE_PATH,
                 file_recv_config=None, debug_msg_counts=Connector.DEBUG_MSG_COUNTS, silent=Connector.SILENT, #use_ack=Connector.USE_ACK,
                 uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET,
                 uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                 hook_server_auth_client=None, enable_client_try_reconnect=True,
                 reuse_server_sockaddr=False, reuse_uds_path_send_to_connector=False, reuse_uds_path_commander_server=False,
                 max_size_file_upload=Connector.MAX_SIZE_FILE_UPLOAD,
                 everybody_can_send_messages=Connector.EVERYBODY_CAN_SEND_MESSAGES,
                 send_message_types_priorities=None):
        
        self.connector_files_dirpath = connector_files_dirpath
        self.default_logger_dirpath = default_logger_dirpath
        self.default_logger_log_level = default_logger_log_level
        if not os.path.isdir(self.connector_files_dirpath):
            os.makedirs(self.connector_files_dirpath)
        if not os.path.isdir(self.default_logger_dirpath):
            os.makedirs(self.default_logger_dirpath)
                
        if not logger:
            if use_default_logger:
                self.logger = get_logger(logfile_path=os.path.join(self.default_logger_dirpath, LOGFILE_DEFAULT_NAME), 
                                         logger_name=DEFAULT_LOGGER_NAME, silent=True, level=default_logger_log_level)
            else:
                self.logger = get_logger(logfile_path=None)  #dummy logger
        else:
            self.logger = logger
            
        self.is_server, self.server_sockaddr, self.use_ssl, self.ssl_allow_all, self.certificates_directory_path = \
                            is_server, server_sockaddr, use_ssl, ssl_allow_all, certificates_directory_path
        self.client_name, self.client_bind_ip = client_name, client_bind_ip
        self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
        self.max_size_file_upload = max_size_file_upload
        self.everybody_can_send_messages = everybody_can_send_messages
        self.send_message_types_priorities = send_message_types_priorities
        self.disk_persistence_send, self.disk_persistence_recv, self.max_size_persistence_path = \
                            disk_persistence_send, disk_persistence_recv, max_size_persistence_path
        self.file_recv_config, self.debug_msg_counts, self.silent = file_recv_config, debug_msg_counts, silent
        self.uds_path_receive_preserve_socket, self.uds_path_send_preserve_socket = \
                            uds_path_receive_preserve_socket, uds_path_send_preserve_socket
        self.hook_server_auth_client, self.enable_client_try_reconnect = hook_server_auth_client, enable_client_try_reconnect
        self.reuse_server_sockaddr, self.reuse_uds_path_send_to_connector, self.reuse_uds_path_commander_server = \
                            reuse_server_sockaddr, reuse_uds_path_send_to_connector, reuse_uds_path_commander_server
                            
        self.config_file_path = config_file_path
        if self.config_file_path:
            self.config_file_path = str(self.config_file_path)
            if os.path.exists(self.config_file_path):
                try:
                    with open(self.config_file_path, 'r') as fd:
                        config_json = json.load(fd)
                        self.logger.info(f'Overriding ConnectorManager attributes {list(config_json.keys())} from '
                                        f'config file {self.config_file_path}')
                        for key,val in config_json.items():                            
                            setattr(self, key, val)
                    #update logger according to config
                    if not logger and use_default_logger:
                        self.logger = get_logger(logfile_path=os.path.join(self.default_logger_dirpath,
                                                 LOGFILE_DEFAULT_NAME), logger_name=DEFAULT_LOGGER_NAME, 
                                                 silent=self.silent, level=self.default_logger_log_level)                            
                except Exception:
                    self.logger.exception('ConnectorManager init config_file_path')
            else:
                self.logger.warning('ConnectorManager init could not find config file at path '+self.config_file_path)

        if self.server_sockaddr:
            self.server_sockaddr = tuple(self.server_sockaddr)

        #source_id is used by send_message, will be overriden by queue_send_to_connector_put if invalid                
        if self.is_server:
            self.source_id = str(self.server_sockaddr)
        else:
            if not self.client_name:
                #raise Exception('Client must have a client_name')
                self.client_name = uuid.uuid4().hex[:8]
                self.logger.warning(f'No client_name provided, using {self.client_name} instead')                
            self.source_id = self.client_name
        
        self.connector = Connector(self.logger, is_server=self.is_server, server_sockaddr=self.server_sockaddr, 
                                   use_ssl=self.use_ssl, ssl_allow_all=self.ssl_allow_all,
                                   certificates_directory_path=self.certificates_directory_path, 
                                   client_name=self.client_name, client_bind_ip=self.client_bind_ip,
                                   send_message_types=self.send_message_types, recv_message_types=self.recv_message_types,
                                   disk_persistence_send=self.disk_persistence_send, 
                                   disk_persistence_recv=self.disk_persistence_recv,
                                   max_size_persistence_path=self.max_size_persistence_path, 
                                   file_recv_config=self.file_recv_config,
                                   debug_msg_counts=self.debug_msg_counts, silent=self.silent, 
                                   connector_files_dirpath=self.connector_files_dirpath, #use_ack=use_ack,
                                   uds_path_receive_preserve_socket=self.uds_path_receive_preserve_socket, 
                                   uds_path_send_preserve_socket=self.uds_path_send_preserve_socket,
                                   hook_server_auth_client=self.hook_server_auth_client, 
                                   enable_client_try_reconnect=self.enable_client_try_reconnect,
                                   reuse_server_sockaddr=self.reuse_server_sockaddr,
                                   reuse_uds_path_send_to_connector=self.reuse_uds_path_send_to_connector,
                                   reuse_uds_path_commander_server=self.reuse_uds_path_commander_server,
                                   max_size_file_upload=self.max_size_file_upload,
                                   everybody_can_send_messages=self.everybody_can_send_messages,
                                   send_message_types_priorities=self.send_message_types_priorities)        
        
            
    async def start_connector(self, delay=None, connector_socket_only=False):        
        if delay:
            self.logger.info(f'Waiting {delay} seconds before starting connector : {self.source_id}')
            await asyncio.sleep(delay)                
        self.logger.info('start_connector : '+str(self.source_id))        
        await self.connector.start(connector_socket_only=connector_socket_only)

    async def stop_connector(self, delay=None, connector_socket_only=False, hard=False, shutdown=False, 
                             enable_delete_files=True):
        if delay:
            self.logger.info(f'Waiting {delay} seconds before stopping connector : {self.source_id}')
            await asyncio.sleep(delay)        
        self.logger.info('stop_connector : '+str(self.source_id))
        await self.connector.stop(connector_socket_only=connector_socket_only, hard=hard, shutdown=shutdown,
                                  enable_delete_files=enable_delete_files)        
        
    async def restart_connector(self, delay=None, sleep_between=0, connector_socket_only=False, hard=False):    
        if delay:
            self.logger.info(f'Waiting {delay} seconds before restarting connector : {self.source_id}')
            await asyncio.sleep(delay)  
        self.logger.info('restart_connector : '+str(self.source_id))            
        await self.connector.restart(sleep_between=sleep_between, connector_socket_only=connector_socket_only, hard=hard)        
        
    def delete_previous_persistence_remains(self):
        self.logger.info(f'{self.source_id} delete_previous_persistence_remains')         
        res = self.connector.delete_previous_persistence_remains()
        return res
    
    async def disconnect_client(self, client_id):
        self.logger.info(f'{self.source_id} disconnect_client {client_id}')
        if self.connector.is_server:
            res = await self.connector.disconnect_client(client_id)
        else:
            res = False
        return res
    
    async def delete_client_certificate(self, client_id, remove_only_symlink=False):
        self.logger.info(f'{self.source_id} delete_client_certificate {client_id} with'
                         f' remove_only_symlink={remove_only_symlink}') 
        if self.connector.is_server:
            res = await self.connector.delete_client_certificate_on_server(client_id, remove_only_symlink)
        else:
            res = await self.connector.delete_client_certificate_on_client()
        return res     
        
    def show_connected_peers(self, dump_result=False):
        self.logger.info(f'{self.source_id} show_connected_peers') 
        return self.connector.show_connected_peers(dump_result=dump_result)

    def ignore_peer_traffic_show(self):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_show') 
        return self.connector.manage_ignore_peer_traffic(show=True)

    def ignore_peer_traffic_enable_unique(self, peername):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_enable_unique {peername}') 
        return self.connector.manage_ignore_peer_traffic(unique_peer=peername)

    def ignore_peer_traffic_disable(self):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_disable') 
        return self.connector.manage_ignore_peer_traffic(disable=True)
    
    def show_log_level(self):
        self.logger.info(f'{self.source_id} show_log_level') 
        return self.connector.show_log_level()

    def set_log_level(self, level):
        self.logger.info(f'{self.source_id} set_log_level') 
        return self.connector.set_log_level(level)       

class ConnectorBaseTool:
    def __init__(self, config_file_path=None, logger=None, use_default_logger=True, default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL,
                 default_logger_dirpath=Connector.CONNECTOR_FILES_DIRPATH, connector_files_dirpath=Connector.CONNECTOR_FILES_DIRPATH, 
                 is_server=False, server_sockaddr=None, client_name=None, 
                 uds_path_receive_preserve_socket=Connector.UDS_PATH_RECEIVE_PRESERVE_SOCKET,
                 uds_path_send_preserve_socket=Connector.UDS_PATH_SEND_PRESERVE_SOCKET,
                 send_message_types=None, recv_message_types=None):

        self.connector_files_dirpath = connector_files_dirpath
        self.default_logger_dirpath = default_logger_dirpath
        if not os.path.isdir(self.connector_files_dirpath):
            os.makedirs(self.connector_files_dirpath)
    
        if not logger:
            if use_default_logger:
                self.logger = get_logger(logfile_path=os.path.join(self.default_logger_dirpath, LOGFILE_DEFAULT_NAME), 
                                         logger_name=DEFAULT_LOGGER_NAME, silent=True, level=default_logger_log_level)                
            else:
                self.logger = get_logger(logfile_path=None)  #dummy logger
        else:
            self.logger = logger
        self.is_server, self.server_sockaddr, self.client_name = is_server, server_sockaddr, client_name
        self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
        self.uds_path_send_preserve_socket = uds_path_send_preserve_socket
        self.uds_path_receive_preserve_socket = uds_path_receive_preserve_socket

        if config_file_path:
            self.config_file_path = str(config_file_path)
            if os.path.exists(self.config_file_path):
                try:
                    with open(self.config_file_path, 'r') as fd:
                        config_json = json.load(fd)
                        self.logger.info(f'Overriding {type(self).__name__} attributes {list(config_json.keys())} '
                                        f'from config file {self.config_file_path}')
                        for key,val in config_json.items():                            
                            setattr(self, key, val)
                    #update logger according to config
                    if not logger and use_default_logger:
                        self.logger = get_logger(logfile_path=os.path.join(self.default_logger_dirpath,
                                                 LOGFILE_DEFAULT_NAME), logger_name=DEFAULT_LOGGER_NAME, 
                                                 silent=self.silent, level=self.default_logger_log_level)                                                        
                except Exception:
                    self.logger.exception('type(self).__name__ init config_file_path')
            else:
                self.logger.warning('type(self).__name__ init could not find config file at path '+self.config_file_path)
        else:
            self.config_file_path = config_file_path

        if self.server_sockaddr:
            self.server_sockaddr = tuple(self.server_sockaddr)        

        #source_id is used by send_message, will be overriden by queue_send_to_connector_put if invalid                
        if self.is_server:
            self.source_id = str(self.server_sockaddr)
        else:
            if not self.client_name:
                #raise Exception('Client must have a client_name')
                self.client_name = uuid.uuid4().hex[:8]
                self.logger.warning(f'No client_name provided, using {self.client_name} instead')                
            self.source_id = self.client_name
        self.reader_writer_uds_path_send = None
        self.message_waiters = {}
        self.connector = Connector(self.logger, tool_only=True, is_server=self.is_server, 
                            server_sockaddr=self.server_sockaddr, connector_files_dirpath=self.connector_files_dirpath, 
                            client_name=self.client_name, send_message_types=self.send_message_types, 
                            recv_message_types=self.recv_message_types)
        
class ConnectorAPI(ConnectorBaseTool):
    '''
    If translating this class to another language (like javascript), it is not necessary to translate the whole Connector class
    which is called here in python just to reuse few of its code.
    The only things from the Connector class that should be translated are :
    - the correct uds_path_send_to_connector string
    - the correct uds_path_receive_from_connector string
    - the value ASYNC_TIMEOUT
    - the values of send_message_types and recv_message_types
    - the functions pack_message and unpack_message
    - the values of classes MessageFields and Structures, which are used by pack_message and unpack_message
    Hence all the initialization arguments received by Connector are also not necessary in another language, as long as
    uds_path_send_to_connector and uds_path_receive_from_connector are hard coded.
    '''
    RECEIVE_FROM_ANY_CONNECTOR_OWNER = True
    
    def __init__(self, *args, receive_from_any_connector_owner = RECEIVE_FROM_ANY_CONNECTOR_OWNER, **kwargs):
        super().__init__(*args, **kwargs)
        self.send_message_lock = asyncio.Lock()
        self.receive_from_any_connector_owner = receive_from_any_connector_owner
        
    async def send_message_await_response(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, wait_for_ack=False):
        res = await self.send_message(await_response=True, message_type=message_type, destination_id=destination_id, 
                                      request_id=request_id, response_id=response_id, data=data, 
                                      data_is_json=data_is_json, binary=binary, with_file=with_file, 
                                      wait_for_ack=wait_for_ack)
        return res
    
    async def send_message(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False):

        try:  
            
            if data_is_json:
                data = json.dumps(data) #, ensure_ascii=False)
            if not self.is_server and not destination_id:
                destination_id = str(self.server_sockaddr)
            self.logger.debug(f'send_message of type {message_type}, destination_id {destination_id}, '
                              f'request_id {request_id}, response_id {response_id}')
                
            message_bytes = self.connector.pack_message(data=data, message_type=message_type, source_id=self.source_id,
                                   destination_id=destination_id, request_id=request_id, response_id=response_id, 
                                   binary=binary, await_response=await_response, with_file=with_file, 
                                   wait_for_ack=wait_for_ack)

            send_message_lock_internally_acquired = False
            if self.uds_path_send_preserve_socket and not await_response:
                #try to reuse connection to uds
                if not self.reader_writer_uds_path_send:     
                    #either there is really no reader_writer_uds_path_send, or the send_message_lock is currently 
                    #locked by another send_message which is in the process of creating a reader_writer_uds_path_send. 
                    #In such a case, we wait for send_message_lock, and check again if reader_writer_uds_path_send exists.
                    try:
                        await asyncio.wait_for(self.send_message_lock.acquire(), Connector.ASYNC_TIMEOUT)
                    except asyncio.CancelledError:
                        raise                        
                    except asyncio.TimeoutError:
                        self.logger.warning('send_message could not acquire send_message_lock')
                        return False
                    else:
                        #reader_writer_uds_path_send may have changed during wait_for(self.send_message_lock.acquire()) : 
                        #checking again if reader_writer_uds_path_send exists
                        if self.reader_writer_uds_path_send:
                            #a new check reader_writer_uds_path_send has just been created by another send_message task : use it !
                            try:
                                self.send_message_lock.release()
                            except Exception:
                                self.logger.exception('send_message_lock release')
                        else:
                            #we acquired send_message_lock, and there is no reader_writer_uds_path_send : 
                            #we set send_message_lock_internally_acquired 
                            #to prevent waiting a second time for send_message_lock in the following
                            send_message_lock_internally_acquired = True
                
                if self.reader_writer_uds_path_send:
                    try:
                        reader, writer = self.reader_writer_uds_path_send
                        writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])    
                        writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])                        
                        await writer.drain()                                        
                        self.logger.debug('send_message reusing existing connection')
                        return True                        
                    except Exception:
                        #now we need to create a new connection
                        self.reader_writer_uds_path_send = None                        
                        self.logger.exception('send_message uds_path_send_preserve_socket')
                        try:
                            writer.close()
                        except Exception:
                            pass
                                          
                        
            self.logger.debug('send_message creating new connection')
            try:
                #in case send_message is called as a task, we need the send_message_lock when creating a 
                #new connection to uds_path_send_to_connector
                #otherwise the order of messages can be messed up. 
                #And also the shared reader_writer_uds_path_send mechanism can be messed up
                if not send_message_lock_internally_acquired:
                    await asyncio.wait_for(self.send_message_lock.acquire(), Connector.ASYNC_TIMEOUT)                
                
                reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=self.connector.uds_path_send_to_connector, 
                                                   limit=Connector.MAX_SOCKET_BUFFER_SIZE), timeout=Connector.ASYNC_TIMEOUT)
                if self.uds_path_send_preserve_socket and not await_response:
                    self.reader_writer_uds_path_send = reader, writer
            except asyncio.CancelledError:
                raise                                            
            except Exception as exc: #ConnectionRefusedError: or TimeoutError
                self.logger.warning(f'send_message could not connect to {self.connector.uds_path_send_to_connector} : {exc}')
                return False                        
            finally:
                try:
                    if self.send_message_lock.locked():
                        self.send_message_lock.release()
                except Exception:
                    self.logger.exception('send_message_lock release')
            
            writer.write(message_bytes[:Structures.MSG_4_STRUCT.size])                                                                
            writer.write(message_bytes[Structures.MSG_4_STRUCT.size:])
            try:
                await asyncio.wait_for(writer.drain(), timeout=Connector.ASYNC_TIMEOUT)
            except asyncio.CancelledError:
                raise                        
            except Exception:
                self.logger.exception('send_message writer drain')
            #beware to not lock the await_response recv_message with send_message_lock
            if await_response:            
                the_response = await self.recv_message(reader, writer)
            self.logger.debug('send_message finished sending')                    
            if await_response:
                writer.close()
                return the_response
            return True      
                
        except asyncio.CancelledError:
            self.logger.warning('send_message : CancelledError')            
            raise            
        except asyncio.IncompleteReadError:
            self.logger.warning('send_message : peer disconnected')
            return False
        except ConnectionResetError as exc:
            self.logger.warning('ConnectionResetError : '+str(exc))
        except Exception as exc:
            self.logger.exception('send_data')
            return False

            
    async def recv_message(self, reader, writer):
        try:
            self.logger.debug('recv_message')
            next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
            next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
            #self.logger.info('Received data from application with length: ' + str(next_length))
            #payload = 2|json|4|data|4|binary
            payload = await asyncio.wait_for(reader.readexactly(next_length), timeout=Connector.ASYNC_TIMEOUT)
            message = next_length_bytes + payload
            response = transport_json , data, binary = self.connector.unpack_message(message)
            self.logger.debug('recv_message : '+str(transport_json))        
            return response
        except asyncio.CancelledError:
            raise        
        except asyncio.IncompleteReadError:
            self.logger.warning('recv_message : peer disconnected')
            return None, None, None
        except ConnectionResetError as exc:
            self.logger.warning('recv_message : peer disconnected '+str(exc))
            return None, None, None            
        except Exception as exc:
            self.logger.exception('recv_message')
            raise            

    async def client_connected_cb(self, message_received_cb, reader, writer):
        #if not os.path.exists('/tmp/yomo'):
        #    with open('/tmp/yomo', 'w') as fd:
        #        fd.write('0')
        while True:
            transport_json , data, binary = await self.recv_message(reader, writer)
            if transport_json:
                #cc = None
                #with open('/tmp/yomo','r') as fd:
                #    aa = fd.read()
                #    if aa:
                #        cc = int(aa) + 1
                #if cc:
                #    print(cc)
                #    with open('/tmp/yomo', 'w') as fd:
                #        fd.write(str(cc))
                    
                await message_received_cb(self.logger, transport_json , data, binary)
            else:
                return
            if not self.uds_path_receive_preserve_socket:
                return
            
    async def start_waiting_for_messages(self, message_type=None, message_received_cb=None, reuse_uds_path=False):
        #message_received_cb must receive arguments transport_json , data, binary
        try:
            uds_path_receive_from_connector = self.connector.uds_path_receive_from_connector.get(message_type)
            if os.path.exists(uds_path_receive_from_connector) and not reuse_uds_path:
                raise Exception(f'{uds_path_receive_from_connector} already in use. Cannot start_waiting_for_messages')
            self.logger.info('start_waiting_for_messages of type {} on socket {}'.format(message_type, 
                             uds_path_receive_from_connector))
            
            if message_type in self.message_waiters:
                raise Exception('Already waiting for messages of type {} on socket {}'.format(message_type, 
                                uds_path_receive_from_connector))
            client_connected_cb = partial(self.client_connected_cb, message_received_cb)
            server = await asyncio.start_unix_server(client_connected_cb, path=uds_path_receive_from_connector, 
                                                     limit=Connector.MAX_SOCKET_BUFFER_SIZE)
            self.message_waiters[message_type] = server
            if self.receive_from_any_connector_owner:
                chown_nobody_permissions(uds_path_receive_from_connector, self.logger)            
            return server
        except asyncio.CancelledError:
            raise        
        except Exception as exc:
            self.logger.exception('start_waiting_for_messages')
            raise

    def stop_waiting_for_messages(self, message_type=None):
        if message_type not in self.message_waiters:
            self.logger.warning('stop_waiting_for_messages has no {} waiter to stop'.format(message_type))
            return
        self.logger.info('stop_waiting_for_messages of type {} on socket {}'.format(message_type, 
                         self.connector.uds_path_receive_from_connector.get(message_type)))        
        server = self.message_waiters.pop(message_type)
        server.close()
        try:
            uds_path_receive_from_connector = self.connector.uds_path_receive_from_connector.get(message_type, '')            
            if os.path.exists(uds_path_receive_from_connector):
                self.logger.info('Deleting file '+ uds_path_receive_from_connector)
                os.remove(uds_path_receive_from_connector)
        except Exception:
            self.logger.exception('stop_waiting_for_messages')
            raise


class ConnectorRemoteTool(ConnectorBaseTool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    async def send_command(self, cmd=None, kwargs=None):
        try:
            if kwargs is None:
                kwargs = {}      
            self.logger.info(f'send_command {cmd} with kwargs {kwargs}')
            message = json.dumps({'cmd':cmd, 'kwargs':kwargs}).encode()
            message = Structures.MSG_4_STRUCT.pack(len(message)) + message
            reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=self.connector.uds_path_commander), 
                                                    timeout=Connector.ASYNC_TIMEOUT)
            writer.write(message)  
            try:
                await asyncio.wait_for(writer.drain(), timeout=Connector.ASYNC_TIMEOUT)
            except Exception:
                self.logger.exception('send_command writer drain')
            next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
            next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
            response = await asyncio.wait_for(reader.readexactly(next_length), timeout=Connector.ASYNC_TIMEOUT)
            writer.close()
            self.logger.info(f'send_command got response {response}')
        except Exception as exc:
            self.logger.exception('send_command')
            response = str(exc).encode()
        return response
            
    
    async def start_connector(self, delay=None):        
        if delay:
            self.logger.info(f'Waiting {delay} seconds before starting connector : {self.source_id}')
            await asyncio.sleep(delay)                
        self.logger.info('start_connector : '+str(self.source_id))
        response = await self.send_command(cmd='start', kwargs={})
        return response        

    async def stop_connector(self, delay=None, hard=False, shutdown=False, enable_delete_files=True,
                             client_wait_for_reconnect=False):
        if delay:
            self.logger.info(f'Waiting {delay} seconds before stopping connector : {self.source_id}')
            await asyncio.sleep(delay)        
        self.logger.info('stop_connector : '+str(self.source_id))
        response = await self.send_command(cmd='stop', kwargs={'hard':hard, 'shutdown':shutdown,
                                                                'enable_delete_files':enable_delete_files,
                                                                'client_wait_for_reconnect':client_wait_for_reconnect})
        return response               
        
    async def restart_connector(self, delay=None, sleep_between=0, connector_socket_only=False, hard=False):    
        if delay:
            self.logger.info(f'Waiting {delay} seconds before restarting connector : {self.source_id}')
            await asyncio.sleep(delay)  
        self.logger.info('restart_connector : '+str(self.source_id))
        response = await self.send_command(cmd='restart', kwargs={'hard':hard, 'sleep_between':sleep_between})
        return response
                
    async def delete_client_certificate(self, client_id=None, remove_only_symlink=False):
        self.logger.info(f'{self.source_id} delete_client_certificate {client_id}')                 
        if self.is_server:
            response = await self.send_command(cmd='delete_client_certificate_on_server', 
                                               kwargs={'client_id':client_id, 'remove_only_symlink':remove_only_symlink})
            return response
        else:
            response = await self.send_command(cmd='delete_client_certificate_on_client', kwargs={})
            return response
        
    async def disconnect_client(self, client_id=None):
        self.logger.info(f'{self.source_id} disconnect_client {client_id}')                         
        if self.is_server:
            response = await self.send_command(cmd='disconnect_client', kwargs={'client_id':client_id})
            return response
        else:
            return False
        
    async def delete_previous_persistence_remains(self):
        self.logger.info(f'{self.source_id} delete_previous_persistence_remains')         
        response = await self.send_command(cmd='delete_previous_persistence_remains__sync', kwargs={})        
        return response
    
    async def show_connected_peers(self):
        self.logger.info(f'{self.source_id} show_connected_peers')         
        response = await self.send_command(cmd='show_connected_peers__sync', kwargs={})        
        return response

    async def peek_queues(self):
        self.logger.info(f'{self.source_id} peek_queues')         
        response = await self.send_command(cmd='peek_queues__sync', kwargs={'dump_result':True})        
        return response
    
    async def ignore_peer_traffic_show(self):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_show')         
        response = await self.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'show':True})        
        return response

    async def ignore_peer_traffic_enable(self):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_enable')         
        response = await self.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'enable':True})        
        return response
    
    async def ignore_peer_traffic_enable_unique(self, peername):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_enable_unique')         
        response = await self.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'unique_peer':peername})        
        return response

    async def ignore_peer_traffic_disable(self):
        self.logger.info(f'{self.source_id} ignore_peer_traffic_disable')         
        response = await self.send_command(cmd='manage_ignore_peer_traffic__sync', kwargs={'disable':True})        
        return response    

    async def show_log_level(self):
        self.logger.info(f'{self.source_id} show_log_level')         
        response = await self.send_command(cmd='show_log_level__sync', kwargs={})        
        return response

    async def set_log_level(self, level):
        self.logger.info(f'{self.source_id} set_log_level {level}')         
        response = await self.send_command(cmd='set_log_level__sync', kwargs={'level':level})        
        return response
    
      

            
            
            
            
            