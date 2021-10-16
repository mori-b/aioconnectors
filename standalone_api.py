'''This file is a simplified standalone version of api.ConnectorAPI
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
    
class Misc:
    CHUNK_INDICATOR = '__aioconnectors_chunk'

    
class ConnectorAPI:

    ASYNC_TIMEOUT = 10        
    MAX_SOCKET_BUFFER_SIZE = 2 ** 16
    UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER = 'uds_path_receive_from_connector_server_{}_{}'    
    UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT = 'uds_path_receive_from_connector_client_{}_{}'    
    UDS_PATH_SEND_TO_CONNECTOR_SERVER = 'uds_path_send_to_connector_server_{}'    
    UDS_PATH_SEND_TO_CONNECTOR_CLIENT = 'uds_path_send_to_connector_client_{}'    
    MAX_LENGTH_UDS_PATH = 104
    RECEIVE_FROM_ANY_CONNECTOR_OWNER = True
    MAX_SIZE_CHUNK_UPLOAD = 1_073_741_824 #1gb
    READ_CHUNK_SIZE = 104_857_600 #100mb
    
    def __init__(self, config_file_path=None, connector_files_dirpath='/var/tmp/aioconnectors', 
                 is_server=False, server_sockaddr=('127.0.0.1',10673), client_name=None, 
                 send_message_types=("event","command"), recv_message_types=("event","command"),
                 uds_path_receive_preserve_socket=True, uds_path_send_preserve_socket=True,
                 receive_from_any_connector_owner=RECEIVE_FROM_ANY_CONNECTOR_OWNER,
                 pubsub_central_broker=False):

        self.connector_files_dirpath = connector_files_dirpath
        if not os.path.isdir(self.connector_files_dirpath):
            os.makedirs(self.connector_files_dirpath)
    

        self.is_server, self.server_sockaddr, self.client_name = is_server, server_sockaddr, client_name
        self.send_message_types, self.recv_message_types = send_message_types, recv_message_types
        self.pubsub_central_broker = pubsub_central_broker        
        self.uds_path_send_preserve_socket = uds_path_send_preserve_socket
        self.uds_path_receive_preserve_socket = uds_path_receive_preserve_socket
        self.receive_from_any_connector_owner = receive_from_any_connector_owner        

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
        
        if self.pubsub_central_broker:
            if self.recv_message_types is None:
                self.recv_message_types = []
            self.recv_message_types.append('_pubsub')        
        if self.send_message_types is None:
            self.send_message_types = []
        #self.send_message_types.append('_pubsub')            
        
        self.uds_path_receive_from_connector = {}
        self.send_message_lock = asyncio.Lock()
        
        if self.is_server:        
            self.alnum_source_id = '_'.join([self.alnum_name(el) for el in self.source_id.split()])            
            self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, 
                                                self.UDS_PATH_SEND_TO_CONNECTOR_SERVER.format(self.alnum_source_id))            
            for recv_message_type in self.recv_message_types:
                self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, 
                                self.UDS_PATH_RECEIVE_FROM_CONNECTOR_SERVER.format(recv_message_type, self.alnum_source_id))
                if len(self.uds_path_receive_from_connector[recv_message_type]) > self.MAX_LENGTH_UDS_PATH:
                    raise Exception(f'{self.uds_path_receive_from_connector[recv_message_type]} is longer '
                                       f'than {self.MAX_LENGTH_UDS_PATH}')                
        else:
            self.alnum_source_id = self.alnum_name(self.source_id)                                        
            self.uds_path_send_to_connector = os.path.join(self.connector_files_dirpath, 
                                                self.UDS_PATH_SEND_TO_CONNECTOR_CLIENT.format(self.alnum_source_id))            
            for recv_message_type in self.recv_message_types:
                self.uds_path_receive_from_connector[recv_message_type] = os.path.join(self.connector_files_dirpath, 
                            self.UDS_PATH_RECEIVE_FROM_CONNECTOR_CLIENT.format(recv_message_type, self.alnum_source_id))
                if len(self.uds_path_receive_from_connector[recv_message_type]) > self.MAX_LENGTH_UDS_PATH:
                    raise Exception(f'{self.uds_path_receive_from_connector[recv_message_type]} is longer '
                                       f'than {self.MAX_LENGTH_UDS_PATH}')                
            
    def alnum_name(self, name):
        return ''.join([str(letter) for letter in name if str(letter).isalnum()])
            
    #4|2|json|4|data|4|binary
    def pack_message(self, transport_json=None, message_type=None, source_id=None, destination_id=None,
                     request_id=None, response_id=None, binary=None, await_response=False,
                     with_file=None, data=None, wait_for_ack=False, message_type_publish=None):
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
        
    async def send_message_await_response(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False, message_type_publish=None, await_response_timeout=None):
        res = await self.send_message(await_response=True, message_type=message_type, destination_id=destination_id,
                                      request_id=request_id, response_id=response_id, data=data, 
                                      data_is_json=data_is_json, binary=binary, with_file=with_file,
                                      wait_for_ack=wait_for_ack, message_type_publish=message_type_publish,
                                      await_response_timeout=await_response_timeout)
        return res

    def send_message_sync(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None,
                           wait_for_ack=False, message_type_publish=None, await_response_timeout=None, loop=None):
        self.logger.debug(f'send_message_sync of type {message_type}, destination_id {destination_id}, '
                          f'request_id {request_id}, response_id {response_id}')
        
        loop = loop or asyncio.get_event_loop()
        send_task = self.send_message(message_type=message_type, destination_id=destination_id, 
                                      request_id=request_id, response_id=response_id, data=data, 
                                      data_is_json=data_is_json, binary=binary, await_response=await_response, 
                                      with_file=with_file, wait_for_ack=wait_for_ack,
                                      message_type_publish=message_type_publish, await_response_timeout=await_response_timeout)
        if loop.is_running():
            return loop.create_task(send_task)
        else:
            return loop.run_until_complete(send_task)    

    async def send_message(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False, message_type_publish=None, await_response_timeout=None, check_chunk_file=True): #, reuse_uds_connection=True):

        if with_file:
            src_path = with_file.get('src_path')
            if src_path and check_chunk_file:
                file_size = os.path.getsize(src_path)
                number_of_chunks, last_bytes_size = divmod(file_size, self.MAX_SIZE_CHUNK_UPLOAD)
                if number_of_chunks:
                    #divide file in chunks of max size MAX_SIZE_CHUNK_UPLOAD, and send each chunk one after the other                    
                    dst_name = with_file.get('dst_name')
                    chunk_basepath = self.connector_files_dirpath #os.path.dirname(src_path)
                    chunk_basename = f'{dst_name}{Misc.CHUNK_INDICATOR}' #f'{dst_name}__aioconnectors_chunk'
                    try:
                        override_src_file_sizes = number_of_chunks * [self.MAX_SIZE_CHUNK_UPLOAD]
                        if last_bytes_size:
                            override_src_file_sizes += [last_bytes_size]
                        len_override_src_file_sizes = len(override_src_file_sizes)
                        chunk_names = []
                        fd = open(src_path, 'rb')
                        for index, chunk_size in enumerate(override_src_file_sizes):
                            chunk_name = f'{chunk_basename}_{index+1}_{len_override_src_file_sizes}'
                            with open(os.path.join(chunk_basepath, chunk_name), 'wb') as fw:
                                number_of_read_chunks, last_size = divmod(chunk_size, self.READ_CHUNK_SIZE)
                                while number_of_read_chunks:                                    
                                    number_of_read_chunks -= 1                                    
                                    chunk_file = fd.read(self.READ_CHUNK_SIZE)    
                                    fw.write(chunk_file)
                                chunk_file = fd.read(last_size)
                                fw.write(chunk_file)

                            self.logger.info(f'send_message of type {message_type}, destination_id {destination_id}, '
                              f'request_id {request_id}, response_id {response_id} creating chunk {chunk_name}')                            
                            chunk_names.append(chunk_name)
                            await asyncio.sleep(0)
                        chunk_file = None
                    except Exception:
                        self.logger.exception('send_message chunks')
                        return False
                    for index, chunk_name in enumerate(chunk_names):
                        chunk_name_path = os.path.join(chunk_basepath, chunk_name)                        
                        with_file['src_path'] = chunk_name_path
                        with_file['dst_name'] = chunk_name
                        with_file['chunked'] = [chunk_basename, index+1, len_override_src_file_sizes]
                        res = await self.send_message(message_type=message_type, destination_id=destination_id,
                                                      request_id=request_id, response_id=response_id,
                                                      data=data, data_is_json=data_is_json, binary=binary,
                                                      await_response=await_response, with_file=with_file, 
                                                      wait_for_ack=wait_for_ack, message_type_publish=message_type_publish,
                                                      await_response_timeout=await_response_timeout, check_chunk_file=False)
                        if os.path.exists(chunk_name_path):
                            if await_response or wait_for_ack:
                                try:
                                    self.logger.info(f'send_message of type {message_type}, destination_id {destination_id}, '
                                  f'request_id {request_id}, response_id {response_id} deleting chunk {chunk_name_path}')
                                    
                                    os.remove(chunk_name_path)
                                except Exception:
                                    self.logger.exception(f'send_message of type {message_type}, destination_id {destination_id}, '
                                  f'request_id {request_id}, response_id {response_id} deleting chunk {chunk_name_path}')
                            else:
                                self.logger.warning(f'send_message of type {message_type}, destination_id {destination_id}, '
                                  f'request_id {request_id}, response_id {response_id} leaving undeleted chunk {chunk_name_path}')                                
                                
                        if not res:
                            return res
                    return res                       
        try:  
            
            if data_is_json:
                data = json.dumps(data) #, ensure_ascii=False)
            if not self.is_server and not destination_id:
                destination_id = str(self.server_sockaddr)
            self.logger.debug(f'send_message of type {message_type}, destination_id {destination_id}, '
                              f'request_id {request_id}, response_id {response_id}')
                
            message_bytes = self.pack_message(data=data, message_type=message_type, source_id=self.source_id,
                                   destination_id=destination_id, request_id=request_id, response_id=response_id, binary=binary,
                                   await_response=await_response, with_file=with_file, wait_for_ack=wait_for_ack,
                                   message_type_publish=message_type_publish)

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
                        try:
                            writer.close()
                        except Exception:
                            pass                                          
                        
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
                if await_response_timeout is not None:
                    try:
                        the_response = await asyncio.wait_for(self.recv_message(reader, writer), timeout=await_response_timeout)
                    except asyncio.TimeoutError:
                        self.logger.warning(f'send_message : await_response_timeout error ({await_response_timeout} s)')
                        writer.close()
                        return False                      
                else:
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

    async def publish_message(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, 
                           wait_for_ack=False):
        res = await self.send_message(message_type='_pubsub', message_type_publish=message_type, destination_id=destination_id, 
                                      request_id=request_id, response_id=response_id, data=data, 
                                      data_is_json=data_is_json, binary=binary, await_response=await_response,
                                      with_file=with_file, wait_for_ack=wait_for_ack)
        return res
    
    def publish_message_sync(self, message_type=None, destination_id=None, request_id=None, response_id=None,
                           data=None, data_is_json=True, binary=None, await_response=False, with_file=None, wait_for_ack=False):
        res = self.send_message_sync(message_type='_pubsub', message_type_publish=message_type,
                                     destination_id=destination_id, request_id=request_id, response_id=response_id,
                                     data=data, data_is_json=data_is_json, binary=binary, await_response=await_response,
                                     with_file=with_file, wait_for_ack=wait_for_ack)
        return res
            
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
            self.message_waiters[message_type] = server
            if self.receive_from_any_connector_owner:            
                chown_nobody_permissions(uds_path_receive_from_connector)   #must be implemented, for example call linux chown
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
            uds_path_receive_from_connector = self.uds_path_receive_from_connector.get(message_type, '')            
            if os.path.exists(uds_path_receive_from_connector):
                self.logger.info('Deleting file '+ uds_path_receive_from_connector)
                os.remove(uds_path_receive_from_connector)
        except Exception:
            self.logger.exception('stop_waiting_for_messages')
            raise

class ConnectorRemoteTool(ConnectorAPI):
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
                                                    timeout=self.ASYNC_TIMEOUT)
            writer.write(message)  
            try:
                await asyncio.wait_for(writer.drain(), timeout=self.ASYNC_TIMEOUT)
            except Exception:
                self.logger.exception('send_command writer drain')
            next_length_bytes = await reader.readexactly(Structures.MSG_4_STRUCT.size)
            next_length = Structures.MSG_4_STRUCT.unpack(next_length_bytes)[0]
            response = await asyncio.wait_for(reader.readexactly(next_length), timeout=self.ASYNC_TIMEOUT)
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
                
    async def delete_client_certificate(self, client_id=None, remove_only_symlink=False, restart_client=True):
        self.logger.info(f'{self.source_id} delete_client_certificate {client_id}')                 
        if self.is_server:
            response = await self.send_command(cmd='delete_client_certificate_on_server', 
                                               kwargs={'client_id':client_id, 'remove_only_symlink':remove_only_symlink})
            return response
        else:
            response = await self.send_command(cmd='delete_client_certificate_on_client', kwargs={'restart_client':restart_client})
            return response
        
    async def disconnect_client(self, client_id=None):
        self.logger.info(f'{self.source_id} disconnect_client {client_id}')                         
        if self.is_server:
            response = await self.send_command(cmd='disconnect_client', kwargs={'client_id':client_id})
            return response
        else:
            return False

    async def blacklist_client(self, client_ip=None, client_id=None):
        self.logger.info(f'{self.source_id} blacklist_client ip : {client_ip}, id : {client_ip}')
        if self.is_server:
            response = await self.send_command(cmd='blacklist_client', kwargs={'client_ip':client_ip, 'client_id':client_id})
            return response
        else:
            return False
        
    async def whitelist_client(self, client_ip=None, client_id=None):
        self.logger.info(f'{self.source_id} whitelist_client ip : {client_ip}, id : {client_ip}')
        if self.is_server:
            response = await self.send_command(cmd='whitelist_client', kwargs={'client_ip':client_ip, 'client_id':client_id})
            return response
        else:
            return False
        
    async def delete_previous_persistence_remains(self):
        self.logger.info(f'{self.source_id} delete_previous_persistence_remains')         
        response = await self.send_command(cmd='delete_previous_persistence_remains__sync', kwargs={})        
        return response
    
    async def show_subscribe_message_types(self):
        self.logger.info(f'{self.source_id} show_subscribe_message_types')         
        response = await self.send_command(cmd='show_subscribe_message_types__sync', kwargs={})        
        return response
    
    async def set_subscribe_message_types(self, *message_types):
        self.logger.info(f'{self.source_id} set_subscribe_message_types {message_types}')         
        response = await self.send_command(cmd='set_subscribe_message_types', kwargs={'message_types':message_types})
        return response
    
    async def show_connected_peers(self, dump_result=False):
        self.logger.info(f'{self.source_id} show_connected_peers')         
        response = await self.send_command(cmd='show_connected_peers__sync', kwargs={'dump_result':False})        
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
