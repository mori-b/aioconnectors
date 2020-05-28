'''This file is a simplified standalone version of connectors_api.ConnectorAPI
    which could be translated to other languages like javascript'''

import os
import asyncio
import json
from structs import Struct
from functools import partial
import uuid

class MessageFields:
    MESSAGE_TYPE = 'message_type'    #'_ssl', '_ack', '_ping', <user-defined>, ...
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
    
class ConnectorAPI:

    ASYNC_TIMEOUT = 10        
    MAX_SOCKET_BUFFER_SIZE = 2 ** 16
    UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER = 'uds_path_receive_from_connector_server_{}_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT = 'uds_path_receive_from_connector_client_{}_{}'    
    UDS_PATH_SEND_TO_CONNECTOR_SERVER = 'uds_path_send_to_connector_server_{}'    
    UDS_PATH_SEND_TO_CONNECTOR_CLIENT = 'uds_path_send_to_connector_client_{}'    

    def __init__(self, config_file_path=None, connector_files_dirpath='/tmp/aioconnectors', 
                 is_server=False, server_sockaddr=('127.0.0.1',12345), client_name=None, 
                 send_message_types=("event","command"), recv_message_types=("event","command"),
                 uds_path_receive_preserve_socket=True, uds_path_send_preserve_socket=True):

        self.connector_files_dirpath = connector_files_dirpath
        if not os.path.isdir(self.connector_files_dirpath):
            os.makedirs(self.connector_files_dirpath)
    

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
        self.uds_path_receive_from_connector = {}
        self.send_message_lock = asyncio.Lock()
        
        if self.is_server:        
            self.alnum_source_id = '_'.join([self.alnum_name(el) for el in self.source_id.split()])            
            self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, 
                                                self.UDS_PATH_SEND_TO_CONNECTOR_SERVER.format(self.alnum_source_id))            
            for recv_message_type in self.recv_message_types:
                self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, 
                                self.UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER.format(recv_message_type, self.alnum_source_id))
        else:
            self.alnum_source_id = self.alnum_name(self.source_id)                                        
            self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, 
                                                self.UDS_PATH_SEND_TO_CONNECTOR_CLIENT.format(self.alnum_source_id))            
            for recv_message_type in self.recv_message_types:
                self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, 
                            self.UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT.format(recv_message_type, self.alnum_source_id))
            
    #4|2|json|4|data|4|binary
    def pack_message(self, transport_json=None, message_type=None, source_id=None, destination_id=None,
                     request_id=None, response_id=None, binary=None, await_response=False,
                     with_file=None, data=None, wait_for_ack=False):
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
        
    async def send_message_await_response(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False):
        res = await self.send_message(await_response=True, message_type=message_type, destination_id=destination_id,
                                      request_id=request_id, response_id=response_id, data=data, 
                                      data_is_json=data_is_json, binary=binary, with_file=with_file,
                                      wait_for_ack=wait_for_ack)
        return res

    async def send_message(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False): #, reuse_uds_connection=True):

        try:  
            
            if data_is_json:
                data = json.dumps(data)
            if not self.is_server and not destination_id:
                destination_id = str(self.server_sockaddr)
            self.logger.debug(f'send_message of type {message_type}, destination_id {destination_id}, request_id {request_id}')
                
            message_bytes = self.pack_message(data=data, message_type=message_type, source_id=self.source_id,
                                   destination_id=destination_id, request_id=request_id, response_id=response_id, binary=binary,
                                   await_response=await_response, with_file=with_file, wait_for_ack=wait_for_ack)

            send_message_lock_internally_acquired = False
            if self.uds_path_send_preserve_socket and not await_response:
                #try to reuse connection to uds
                if not self.reader_writer_uds_path_send:     
                    #either there is really no reader_writer_uds_path_send, or the send_message_lock is currently 
                    #locked by another send_message which is
                    #in the process of creating a reader_writer_uds_path_send.
                    #In such a case, we wait for send_message_lock, and check again if reader_writer_uds_path_send exists.
                    try:
                        await asyncio.wait_for(self.send_message_lock.acquire(), self.ASYNC_TIMEOUT)                
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
                                          
                        
            self.logger.debug('send_message creating new connection')
            try:
                #in case send_message is called as a task, we need the send_message_lock when 
                #creating a new connection to uds_path_send_to_connector
                #otherwise the order of messages can be messed up. 
                #And also the shared reader_writer_uds_path_send mechanism can be messed up
                if not send_message_lock_internally_acquired:
                    await asyncio.wait_for(self.send_message_lock.acquire(), self.ASYNC_TIMEOUT)                
                
                reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path=self.uds_path_send_to_connector, 
                                                   limit=self.MAX_SOCKET_BUFFER_SIZE), timeout=self.ASYNC_TIMEOUT)
                if self.uds_path_send_preserve_socket and not await_response:
                    self.reader_writer_uds_path_send = reader, writer
            except asyncio.CancelledError:
                raise                                            
            except Exception as exc: #ConnectionRefusedError: or TimeoutError
                self.logger.warning(f'send_message could not connect to {self.uds_path_send_to_connector} : {exc}')
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
                await asyncio.wait_for(writer.drain(), timeout=self.ASYNC_TIMEOUT)
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
            payload = await asyncio.wait_for(reader.readexactly(next_length), timeout=self.ASYNC_TIMEOUT)
            message = next_length_bytes + payload
            response = transport_json , data, binary = self.unpack_message(message)
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
        while True:
            transport_json , data, binary = await self.recv_message(reader, writer)
            if transport_json:
                await message_received_cb(self.logger, transport_json , data, binary)
            else:
                return
            if not self.uds_path_receive_preserve_socket:
                return
            
    async def start_waiting_for_messages(self, message_type=None, message_received_cb=None, reuse_uds_path=False):
        #message_received_cb must receive arguments transport_json , data, binary
        try:
            uds_path_receive_from_connector = self.uds_path_receive_from_connector.get(message_type)
            if os.path.exists(uds_path_receive_from_connector) and not reuse_uds_path:
                raise Exception(f'{uds_path_receive_from_connector} already in use. Cannot start_waiting_for_messages')
            self.logger.info('start_waiting_for_messages of type {} on socket {}'.format(message_type, 
                             uds_path_receive_from_connector))
            
            if message_type in self.message_waiters:
                raise Exception('Already waiting for messages of type {} on socket {}'.format(message_type, 
                                uds_path_receive_from_connector))
            client_connected_cb = partial(self.client_connected_cb, message_received_cb)
            server = await asyncio.start_unix_server(client_connected_cb, path=uds_path_receive_from_connector, 
                                                     limit=self.MAX_SOCKET_BUFFER_SIZE)
            chown_nobody_permissions(uds_path_receive_from_connector)   #must be implemented, for example call linux chown
            self.message_waiters[message_type] = server
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
                         self.uds_path_receive_from_connector.get(message_type)))        
        server = self.message_waiters.pop(message_type)
        server.close()
        try:
            for the_path in self.uds_path_receive_from_connector.values():
                if os.path.exists(the_path):
                    self.logger.info('Deleting file '+ the_path)
                    os.remove(the_path)
        except Exception:
            self.logger.exception('stop_waiting_for_messages')
            raise

