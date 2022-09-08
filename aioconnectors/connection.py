'''
Protocol between client and server : 
4 bytes = struct 'I' for size up to 2**32-1 = 4294967295 = 4GB
2 bytes = struct 'H' for size up to 2**16-1 = 65536 = 65 KB

network layer : 
4 bytes = length, rest is data 

transport layer :
2 bytes = length, the rest is a json descriptor like : {'message_type':'any', 
        'source_id':'', 'destination_id':'', 'request_id':<int>, 'response_id':<int>, 'with_binary':true,
        'with_file':{'src_path':'','dst_type':'', 'dst_name':'', 'delete':False, 'owner':'', 'file_error':''},
        'await_response':<bool>, 'wait_for_ack':<bool>}
request_id and response_id are relevant for application layer

application layer :
4 bytes = length of json following, rest is custom json
if with_binary key exists :
4 bytes = length of binary data following, rest is binary data

'''

import asyncio
import stat
from struct import Struct
import json
import os
import ssl
import ipaddress
import re
import hashlib
import secrets
import socket
import uuid

from .helpers import full_path, chown_file, validate_source_id, CustomException, PYTHON_GREATER_37


DEBUG_SHOW_DATA = False

class MessageFields:
    MESSAGE_TYPE = 'message_type'    #'_ssl', '_ack', '_ping', '_handshake_ssl', '_handshake_no_ssl', '_token', '_pubsub', <user-defined>, ...
    SOURCE_ID = 'source_id'    #str
    DESTINATION_ID = 'destination_id'    #str
    REQUEST_ID = 'request_id'    #int
    RESPONSE_ID = 'response_id'    #int
    WITH_BINARY = 'with_binary'    #boolean
    AWAIT_RESPONSE = 'await_response'    #boolean
#dict {'src_path':<str>, 'dst_name':<str>, 'dst_type':<str>, 'binary_offset':<int>, 'delete':<boolean>, 'owner':<user>:<group>}    
    WITH_FILE = 'with_file'    
    TRANSPORT_ID = 'transport_id'    #int
    WAIT_FOR_ACK = 'wait_for_ack'    #boolean
    MESSAGE_TYPE_PUBLISH = 'message_type_publish'
    ERROR = 'error'
    
class Structures:
    MSG_4_STRUCT = Struct('I')    #4
    MSG_2_STRUCT = Struct('H')    #2
#MSG_LENGTH_STRUCT = Struct('Q')    #8
    
class Misc:
    CHUNK_INDICATOR = '__aioconnectors_chunk'
    
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
        self.keep_alive_event_received = asyncio.Event()    #client only
        self.send_message_lock = asyncio.Lock()

    def stop_nowait_for_persistence(self, message_tuple=None):
        #at disconnection, this functions stores queue_send remainings into persistence
        #if exists, this is the first data to be stored into persistence
        #message_tuple if exists is the last message processed during disconnection in handle_outgoing_connection_queue,
        #and will then be the first message stored into persistence
        try:
            self.logger.info(f'{self.connector.source_id} stop_nowait_for_persistence')
            #remove queue (for queue_send_to_connector_put) and send remainings (if exist) of queue_send to persistence file
            self.connector.full_duplex_connections.pop(self.peername, None)            
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
                    if self.connector.SUPPORT_PRIORITIES:
                        priority, the_time, (transport_json, data, binary) = queue_send.get_nowait()
                    else:
                        transport_json, data, binary = queue_send.get_nowait()
                    disk_persistence = True
                    if disk_persistence_is_list:
                        #disk_persistence can be a list of message types
                        disk_persistence = (transport_json.get(MessageFields.MESSAGE_TYPE) in self.connector.disk_persistence)
                    if disk_persistence:                    
                        count += 1
                        message = self.connector.pack_message(transport_json=transport_json,
                                                              data=data, binary=binary)
                        self.logger.info(f'Emptying queue_send, Storing message number {count} to persistence '
                                         f'to peername {self.peername}')
                        self.connector.store_message_to_persistence(self.peername, message)                    
        except asyncio.CancelledError:
            raise                          
        except Exception:
            self.logger.exception(f'{self.connector.source_id} stop_nowait_for_persistence')            
            
        
    def stop_task(self, client_wait_for_reconnect=False):
        self.loop.create_task(self.stop(client_wait_for_reconnect=client_wait_for_reconnect))
        
    async def stop(self, client_wait_for_reconnect=False, hard=False):
        try:
            if not self.peername:
                self.peername = uuid.uuid4().hex
            
            if not self.connector.is_server:
                if self.connector.subscribe_message_types:
                    try:
                        await self.send_message(message_type='_pubsub',
                                    data=json.dumps({'unsubscribe_message_types':self.connector.subscribe_message_types}))
                    except Exception:
                        self.logger.exception(self.connector.source_id+' stop')                    
            
            self.is_stopping = True
            self.logger.info(f'{self.connector.source_id} stop FullDuplex to peer {self.peername}')
            self.logger.info(f'queue_recv size {self.connector.queue_recv.qsize()} , queue_send size '
    f'{self.connector.queue_send[self.peername].qsize() if self.peername in self.connector.queue_send else "popped"}')            
            #first, cancel incoming task to stop receiving messages from peer into queue_recv
            self.connector.cancel_tasks(task_names=[self.peername+'_incoming'])
            #we never stop listening to uds queue_send_to_connector,
            #but we remove peername from queue_send so that it stops writing messages into queue_send
            #this has been done already in stop_nowait_for_persistence in case of persistence, but need to be done here otherwise
            
            #stop_nowait_for_persistence is not necessarily called during handle_ssl and no_ssl_handshake, so 
            #pop full_duplex_connections always : at worst it will try to pop again.
            self.connector.full_duplex_connections.pop(self.peername, None)                
            
            if not self.connector.disk_persistence:            
                #this has been done already in stop_nowait_for_persistence if disk_persistence
                self.logger.info(f'{self.connector.source_id} stop, now deleting queue_send of peer {self.peername}')
#                self.connector.full_duplex_connections.pop(self.peername, None)                
                queue_send = self.connector.queue_send.pop(self.peername, None)
                if queue_send:
                    if hard:
                        self.logger.info(f'{self.connector.source_id} not waiting for queue_send to empty (hard stop), '
                                         f'with peer {self.peername}')                                
                    else:
                        self.logger.info(f'{self.connector.source_id} waiting for queue_send to empty (soft stop), '
                                         f'with peer {self.peername}')                                
                        #let handle_outgoing_connection send remainings of queue_send into peer                        
                        await queue_send.join()

            if hard:
                self.logger.info(f'{self.connector.source_id} not waiting for queue_recv to empty (hard stop), '
                                 f'with peer {self.peername}')                                
            else:
                self.logger.info(f'{self.connector.source_id} waiting for queue_recv to empty (soft stop), '
                                 f'with peer {self.peername}')                                
                #let queue_recv_from_connector read all messages from queue_recv                
                await self.connector.queue_recv.join()

            if hard:
                self.logger.info(f'{self.connector.source_id} stop, without waiting for queues to peer {self.peername}')    
            else:
                self.logger.info(f'{self.connector.source_id} stop, after waiting for queues to peer {self.peername}')    
            self.logger.info(f'queue_recv size {self.connector.queue_recv.qsize()} , queue_send size '
        f'{self.connector.queue_send[self.peername].qsize() if self.peername in self.connector.queue_send else "popped"}')            
            
            self.writer.close()
            if PYTHON_GREATER_37:
                try:
                    await self.writer.wait_closed()      #python 3.7                                              
                except Exception as exc:
                    #cath exception because of scenario seen in TEST_PERSISTENCE_CLIENT with TEST_WITH_SSL=False
                    self.logger.warning(self.connector.source_id+' stop wait_closed : '+str(exc))
                    
            self.loop.create_task(self.connector.cancel_tasks_async(task_names=[self.peername+'_incoming', 
                                                                                self.peername+'_outgoing']))        
            
            if client_wait_for_reconnect and not self.connector.is_server:
                #try to restart the client if server disconnected
                self.connector.tasks['client_wait_for_reconnect'] = \
                    self.loop.create_task(self.connector.client_wait_for_reconnect())    
            
        except Exception as exc:
            self.logger.exception(self.connector.source_id+' stop : '+str(exc))
            
    async def start(self):
        try:
            self.logger.info(f'{self.connector.source_id} start FullDuplex')
            self.peername = None
            self.is_stopping = False            
            peer_identification_finished = False
            if self.connector.is_server:
                if self.connector.use_ssl:
                    if not self.connector.ssl_allow_all:
                        peer_cert = self.writer.get_extra_info('ssl_object').getpeercert()
    # {'subject': ((('organizationName', '{}'),), (('commonName', '{}'),)), 'issuer': ((('organizationName', '{}'),), (('commonName', '{}'),)), 'version': 1, 'serialNumber': '8F7A25089D8D4DF0F3FE6CE5B1DA059C7D6837', 'notBefore': 'Feb 25 10:20:26 2020 GMT', 'notAfter': 'Mar 26 10:20:26 2020 GMT'}
                        #for client peer validation
                        #client_certificate_common_name = peer_cert["subject"][1][0][1]
                        
                        self.client_certificate_id = peer_cert['serialNumber']
    
                        if self.client_certificate_id not in self.connector.ssl_helper.default_client_cert_ids_list:                            
                            peername = self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(self.client_certificate_id)
                            if not peername:
                                self.logger.error(f'Authorized client with certificate '
                                                  f'{self.client_certificate_id} has no source_id ! Aborting')
                                self.peername = str(self.writer.get_extra_info('peername'))
                                raise Exception('Unknown client')
                            peer_identification_finished = True     
                            old_peername = str(self.writer.get_extra_info('peername'))                                                        
                            self.logger.info(f'Replacing peername {old_peername} in full_duplex_connections with {peername}')
                            self.connector.full_duplex_connections[peername] = self.connector.full_duplex_connections.pop(old_peername)

                        else:
                            #we could use the default_client_cert_id, but prefer to have a unique peername per client
                            #for rare case where 2 clients are connecting simultaneously and have same default_client_cert_id
                            peername = str(self.writer.get_extra_info('peername'))
                            #for client with default certificate, creation of handle_outgoing_connection task 
                            #is not necessary and not performed
                    else:
                        #in ssl_allow_all mode, no cert can be obtained, peer_identification_finished will be 
                        #finished in handle_ssl_messages_server
                        self.main_case_receive_first_token_command_from_client = True                        
                        peername = str(self.writer.get_extra_info('peername'))
                        
                        if self.connector.use_token:
                            peer_cert = self.writer.get_extra_info('ssl_object').getpeercert() or {}
                            #client_certificate_id can be useful in server token mode to allow non default client
                            self.client_certificate_id = peer_cert.get('serialNumber', None)
                            if self.client_certificate_id and self.connector.token_verify_peer_cert \
                                and self.connector.token_server_allow_authorized_non_default_cert and ( \
                                self.client_certificate_id not in self.connector.ssl_helper.default_client_cert_ids_list):                                
                                peername = self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(self.client_certificate_id)
                                if not peername:
                                    self.logger.error(f'Authorized client with certificate '
                                                      f'{self.client_certificate_id} has no source_id ! Aborting')
                                    self.peername = str(self.writer.get_extra_info('peername'))
                                    raise Exception('Unknown client')
                                self.logger.info(f'{self.connector.source_id} handle_incoming_connection accepting without token '
                                                 f'authorized client {peername} because token_verify_peer_cert '
                                                 'and token_server_allow_authorized_non_default_cert')                                    
                                peer_identification_finished = True     
                                old_peername = str(self.writer.get_extra_info('peername'))                                                        
                                self.logger.info(f'Replacing peername {old_peername} in full_duplex_connections with {peername}')
                                self.connector.full_duplex_connections[peername] = self.connector.full_duplex_connections.pop(old_peername)                               
                                #in token_allow_non_default_cert mode, we allow authorized non default certificate 
                                self.main_case_receive_first_token_command_from_client = False
                        
                else:
                    #this creates a temporary entry in queue_send, peername will be replaced by client_id after handshake_no_ssl
                    peername = str(self.writer.get_extra_info('peername'))     
                    #for server without ssl, creation of handle_outgoing_connection task will be done in handle_handshake_no_ssl_server
            else:
                peername = str(self.connector.server_sockaddr)
                peer_identification_finished = True
                
            if peername+'_incoming' in self.connector.tasks:
                #problem here in case of server, after client without ssl had its peername replaced. 
                #we won't detect the redundant connection
                self.logger.warning('peername : '+str(peername)+' already connected : Disconnecting and reconnecting...')
                self.connector.cancel_tasks(task_names=[peername+'_incoming', peername+'_outgoing'])
                
            self.peername = peername
            self.extra_info = self.writer.get_extra_info('peername')

            if self.connector.is_server and self.connector.hook_server_auth_client:
                accept = await self.connector.hook_server_auth_client(self.peername)
                if accept:
                    self.logger.info(f'{self.connector.source_id} accepting client {self.peername}')
                else:
                    self.logger.info(f'{self.connector.source_id} blocking client {self.peername}')
                    await self.stop()
                    return

            if self.connector.is_server:
                
                if self.connector.whitelisted_clients_id or self.connector.whitelisted_clients_ip \
                                        or self.connector.whitelisted_clients_subnet:
                    allow_id = allow_ip = allow_subnet = True
                                        
                    if self.connector.whitelisted_clients_ip:
                        allow_ip = False
                        if self.extra_info[0] in self.connector.whitelisted_clients_ip:
                            self.logger.info(f'{self.connector.source_id} allowing whitelisted client {self.peername}'
                                             f' because of ip {self.extra_info}')
                            allow_ip = True

                    if self.connector.whitelisted_clients_subnet:
                        allow_subnet = False
                        for subnet in self.connector.whitelisted_clients_subnet:
                            if ipaddress.IPv4Address(self.extra_info[0]) in subnet:
                                self.logger.info(f'{self.connector.source_id} allowing whitelisted client {self.peername}'
                                                 f' because of ip subnet {self.extra_info}')
                                allow_subnet = True
                                break

                    if peer_identification_finished:
                        if self.connector.whitelisted_clients_id:
                            allow_id = False                                        
                            for maybe_regex in self.connector.whitelisted_clients_id:
                                if re.match(maybe_regex, self.peername):                            
                                    self.logger.info(f'{self.connector.source_id} allowing whitelisted client'
                                                     f' {self.peername} from ip {self.extra_info}')
                                    allow_id = True
                                    break
                            
                    if not all([allow_id, allow_ip, allow_subnet]):
                        self.logger.info(f'{self.connector.source_id} blocking non whitelisted client {self.peername} from'
                                         f' ip {self.extra_info}, allow id:{allow_id}, ip:{allow_ip}, subnet:{allow_subnet}')                        
                        await self.stop()
                        if self.connector.hook_whitelist_clients:
                            #here the hook might update some db that might in return send add_whitelist_client
                            #to let this client connect successfully next try (5s)
                            await self.connector.hook_whitelist_clients(self.extra_info, self.peername)
                        return                    
                        
                if peer_identification_finished:                    
                    if self.connector.blacklisted_clients_id:
                        for maybe_regex in self.connector.blacklisted_clients_id:
                            if re.match(maybe_regex, self.peername):
                                self.logger.info(f'{self.connector.source_id} blocking blacklisted'
                                                 f' client {self.peername} from ip {self.extra_info}')
                                await self.stop()
                                return
                
                if self.connector.blacklisted_clients_ip:
                    if self.extra_info[0] in self.connector.blacklisted_clients_ip:
                        self.logger.info(f'{self.connector.source_id} blocking blacklisted client {self.peername}'
                                         f' because of ip {self.extra_info}')
                        await self.stop()
                        return             

                if self.connector.blacklisted_clients_subnet:
                    for subnet in self.connector.blacklisted_clients_subnet:
                        if ipaddress.IPv4Address(self.extra_info[0]) in subnet:
                            self.logger.info(f'{self.connector.source_id} blocking blacklisted client {self.peername}'
                                             f' because of ip subnet {self.extra_info}')
                            await self.stop()
                            return             

            if peer_identification_finished:
                self.logger.info(f'{self.connector.source_id} start FullDuplex peer_identification_finished for {self.peername}'
                                 f' from {str(self.writer.get_extra_info("peername"))}')
            else:
                self.logger.info(f'{self.connector.source_id} start FullDuplex peer identification not finished yet '
                                 f'for {self.peername}')
                                        
            if self.peername not in self.connector.queue_send:
                self.logger.info(self.connector.source_id+' Creating queue_send for peername : '+str(self.peername))
                if self.connector.SUPPORT_PRIORITIES:
                    self.connector.queue_send[self.peername] = asyncio.PriorityQueue(maxsize=self.connector.MAX_QUEUE_SIZE)  
                else:
                    self.connector.queue_send[self.peername] = asyncio.Queue(maxsize=self.connector.MAX_QUEUE_SIZE)                     
                
            #self.lock_connection = asyncio.Lock()    #to not mix send and recv internal steps
            task_incoming_connection = self.loop.create_task(self.handle_incoming_connection())
            self.connector.tasks[self.peername+'_incoming'] = task_incoming_connection
            
            if peer_identification_finished:
                task_outgoing_connection = self.loop.create_task(self.handle_outgoing_connection())
                self.connector.tasks[self.peername+'_outgoing'] = task_outgoing_connection
                
            self.logger.info(f'{self.connector.source_id} start FullDuplex, Now tasks are : '
                             f'{list(self.connector.tasks.keys())}')
        except asyncio.CancelledError:
            raise                        
        except Exception:
            self.logger.exception(f'{self.connector.source_id} start FullDuplex')            
            if self.connector.disk_persistence:
                self.stop_nowait_for_persistence()
            await self.stop()            
            
        
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
        
            if self.connector.subscribe_message_types:
                try:
                    await self.send_message(message_type='_pubsub',
                                data=json.dumps({'subscribe_message_types':self.connector.subscribe_message_types}))
                except Exception:
                    self.logger.exception(self.connector.source_id+' handle_incoming_connection')                    
                    return                   
                
        else:
            if self.connector.use_ssl:                
                if self.connector.use_token:                        
                    if self.main_case_receive_first_token_command_from_client:
                        self.logger.info(self.connector.source_id+' handle_incoming_connection waiting for message with use_token')
                        transport_json, data, binary = await self.recv_message()
                        message_type = transport_json.get(MessageFields.MESSAGE_TYPE)                                    
                        #here message_type should be _token, which is validated inside handle_ssl_messages_server
                        await self.handle_ssl_messages_server(data, transport_json)
                        #don't send _token messages to queues                                    
        
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
                        
                self.logger.debug(f'{self.connector.source_id} handle_incoming_connection received from peer : '
                                  f'{json.dumps(transport_json)}')
                if DEBUG_SHOW_DATA:
                    self.logger.info('handle_incoming_connection received data from peer : ' + str(data))
                    if binary:
                        self.logger.info('handle_incoming_connection received binary from peer : ' + str(binary))                    
                
                if self.connector.is_server:
                    if self.connector.use_ssl and not self.connector.ssl_allow_all: 
                        #check if client_certificate_id in peer client certificate is the id of the certificate
                        #created by server for the requested source_id
                        if self.client_certificate_id != \
                       self.connector.ssl_helper.source_id_2_cert['source_id_2_cert'].get(transport_json[MessageFields.SOURCE_ID]):
                           self.logger.warning('Client {} from {} tried to impersonate client {}'.format(\
                                            self.connector.ssl_helper.source_id_2_cert['cert_2_source_id'].get(\
                                                        self.client_certificate_id), self.extra_info, transport_json[MessageFields.SOURCE_ID]))
                           continue
                           #self.stop_task()
                           #return                
                
                if message_type == '_ack':     
                    #if ACK is received, update accordingly the ack_dict
                    #this could be checked only when use_ack is true, 
                    #because for it to happen we must have sent transport_id beforehand                    
                    transport_id = transport_json.pop(MessageFields.TRANSPORT_ID, None)                                        
                    if transport_id and (transport_id in self.ack_dict):
                        self.ack_dict[transport_id].set()
                    else:
                        self.logger.warning(f'handle_incoming_connection ACK received from peer {self.peername} '
                                            f'with unknown transport_id {transport_id}. Ignoring...')
                    continue
                elif message_type == '_ping':
                    #if _ping is received with await_response, it is a ping request : 
                    #reply immediately WITHOUT await_response, to prevent ping infinite loop.
                    #if _ping is received without await_response, it is a ping reply, that we forward as usual
                    if transport_json.get(MessageFields.AWAIT_RESPONSE):
                        transport_json_ping_reply = {MessageFields.MESSAGE_TYPE:'_ping', 
                                                     MessageFields.RESPONSE_ID:transport_json[MessageFields.REQUEST_ID]}   
                        await self.send_message(transport_json=transport_json_ping_reply, data=data, binary=binary)
                        transport_json, data, binary = None, None, None
                        continue
                    else:
                        if transport_json.get(MessageFields.RESPONSE_ID) == self.connector.KEEP_ALIVE_CLIENT_REQUEST_ID:
                            transport_json, data, binary = None, None, None
                            self.keep_alive_event_received.set()
                            continue
                
                #at this stage, the message received is never an ACK                
                transport_id = transport_json.pop(MessageFields.TRANSPORT_ID, None)                    
                if transport_id:
                    #if peer sent a transport_id, we must answer ACK
                    #we could have done this only if use_ack is true, but now it means that we are not dependent 
                    #on configuration to support peers having use_ack true
                    self.logger.info(f'handle_incoming_connection {self.connector.source_id} Sending ACK to '
                                     f'peer {self.peername} for transport id {transport_id}')
                    transport_json_ack = {MessageFields.TRANSPORT_ID:transport_id, MessageFields.MESSAGE_TYPE:'_ack'}
                    await self.send_message(transport_json=transport_json_ack)                                           
                
                if self.connector.ignore_peer_traffic:
                    ignore_peer_traffic = False
                    if self.connector.ignore_peer_traffic is True:
                        ignore_peer_traffic = True
                    else:
                        ignore_peer_traffic = (self.peername in self.connector.ignore_peer_traffic)
                    if ignore_peer_traffic:
                        self.logger.debug(f'{self.connector.source_id} handle_incoming_connection : '
                                          f'Ignoring message from peer {self.peername}')
                        transport_json, data, binary = None, None, None
                        continue
                    
                if (message_type not in self.connector.recv_message_types) and (message_type != '_ping'):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer '
                                        f'{self.peername} received a message with invalid type {message_type}. '
                                        'Ignoring...')   
                    put_msg_to_queue_recv = False
                else:
                    put_msg_to_queue_recv = True
                    
                if put_msg_to_queue_recv:
                    #dump file if with_file before sending message_tuple to queue
                    with_file = transport_json.get(MessageFields.WITH_FILE)
                    #dict {'src_path':, 'dst_name':, 'dst_type':, 'binary_offset':, 'owner':'user:group'}
                    if with_file:
                        file_recv_config = self.connector.file_recv_config.get(with_file.get('dst_type'))
                        file_owner = file_recv_config.get('owner')
                        override_existing = file_recv_config.get('override_existing', False)

                        dst_dirpath = file_recv_config['target_directory'].format(**transport_json)
                        try:
                            if self.connector.hook_target_directory:
                                hook_target_directory = self.connector.hook_target_directory.get(with_file['dst_type'])                                
                                #user should have manually defined hook_target_directory(transport_json) beforehand
                                if hook_target_directory:
                                    hooked_target_directory = hook_target_directory(transport_json)
                                    if hooked_target_directory:
                                        dst_dirpath = os.path.join(dst_dirpath, hooked_target_directory)
                                    elif hooked_target_directory is False:
                                        #hook_target_directory returning False means refuse this file
                                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer '
                                                    f'{self.peername} tried to create file in directory refused by hooked_target_directory'
                                                    f' {dst_dirpath} for type {with_file.get("dst_type")}, ignoring...')                                    
                                        dst_dirpath = False
                                    #else, hook_target_directory returning None means no change to dst_dirpath                                        
                        except Exception:
                            self.logger.exception('hook_target_directory')
                        
                        if dst_dirpath:
                            self.logger.info(f'{self.connector.source_id} handle_incoming_connection from peer '
                                        f'{self.peername} storing file into {dst_dirpath}')                            
                            binary_offset = 0
                            try:
                                if not binary:
                                    binary = b''
                                binary_offset = with_file.get('binary_offset', 0)                            
                                dst_fullpath = full_path(os.path.join(dst_dirpath, with_file.get('dst_name','')))
                                if not dst_fullpath.startswith(dst_dirpath):
                                    raise CustomException(f'Illegal traversal file path {dst_fullpath}')
                                if with_file.get('file_error'):
                                    raise CustomException(f'File error : {with_file["file_error"]}')
                                continue_with_file = True
                                if os.path.exists(dst_fullpath):
                                    if not override_existing:
                                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                                        f'peer {self.peername} trying to override existing file '
                                                        f'{dst_fullpath}, ignoring...')
                                        continue_with_file = False
                                if continue_with_file:
                                    self.logger.info(f'{self.connector.source_id} handle_incoming_connection from peer '
                                                     f'{self.peername} writing received file to {dst_fullpath}')
                                    file_owner_from_client = with_file.get('owner')
                                    if file_owner_from_client:
                                        #file_owner_from_client takes precedence over file_owner
                                        self.logger.info(f'{self.connector.source_id} handle_incoming_connection from peer '
                                                     f'{self.peername} using file_owner_from_client {file_owner_from_client}')                                        
                                        file_owner = file_owner_from_client
                                    if file_owner:
                                        file_owner = file_owner.split(':', maxsplit=1)
                                        if len(file_owner) != 2:
                                            self.logger.warning(f'{self.connector.source_id} from peer {self.peername} '
                                                            f'Invalid file owner in {with_file}')
                                            file_owner = None                                                
                                    #before creating new file, create its dirnames if new
                                    dir_to_test = os.path.dirname(dst_fullpath)
                                    dirs_to_create = []
                                    while not os.path.exists(dir_to_test):
                                        dirs_to_create.append(dir_to_test)
                                        dir_to_test = os.path.dirname(dir_to_test)
                                    dirs_to_create = dirs_to_create[::-1]
                                    for dir_to_create in dirs_to_create:
                                        self.logger.info(f'{self.connector.source_id} from peer {self.peername} '
                                                           f'creating directory {dir_to_create}')                                        
                                        os.mkdir(dir_to_create)
                                        if file_owner:
                                            chown_file(dir_to_create, *file_owner, self.logger)
                                    #if not os.path.exists(dir_dst_fullpath):
                                    #    self.logger.info(f'{self.connector.source_id} from peer {self.peername} '
                                    #                        'fcreating directory {dir_dst_fullpath}')
                                    #    os.makedirs(dir_dst_fullpath)
                                    
                                    if len(binary[binary_offset:]) > self.connector.max_size_file_upload_recv:
                                        raise CustomException(f'{self.connector.source_id} cannot receive too large file of size'
                                                        f' {len(binary[binary_offset:])}')                                    
                                                                        
                                    with open(dst_fullpath, 'wb') as fd:
                                        fd.write(binary[binary_offset:])
                                    if self.connector.pubsub_central_broker:
                                        with_file['src_path'] = dst_fullpath    #get ready to resend file to subscribers
                                    else:
                                        if file_owner:
                                            chown_file(dst_fullpath, *file_owner, self.logger)
                                        try:
                                            chunked = with_file.get('chunked')
                                            if chunked:
                                                self.logger.debug(f'{self.connector.source_id} handle_incoming_connection from '
                                                                f'peer {self.peername} received with_file with chunked '
                                                                f'{chunked}')
                                                #chunked looks like : [chunk_basename, index+1, len_override_src_file_sizes]
                                                chunk_basename, chunk_index, number_of_chunks = chunked
                                                dst_dir_files = [thefile for thefile in os.listdir(dir_to_test) if \
                                                                 thefile.startswith(chunk_basename)]
                                                if len(dst_dir_files) == number_of_chunks:
                                                    files_to_unify = sorted([os.path.join(dir_to_test, thefile) for thefile \
                                                                       in dst_dir_files])                                                    
                                                    dst_filename = chunk_basename.split(Misc.CHUNK_INDICATOR)[0]
                                                    dst_filename_fullpath = full_path(os.path.join(dst_dirpath, dst_filename))
                                                    unify_chunks = True
                                                    if os.path.exists(dst_filename_fullpath):
                                                        if not override_existing:
                                                            self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                                                            f'peer {self.peername} trying to override existing file '
                                                                            f'{dst_filename_fullpath}, ignoring...')
                                                            unify_chunks = False
                                                    if unify_chunks:
                                                        total_file_size = sum([os.path.getsize(chunk) for chunk in files_to_unify])
                                                        if total_file_size > self.connector.max_size_file_upload_recv:
                                                            self.logger.warning(f'{self.connector.source_id} cannot receive too large file of size'
                                                                            f' {total_file_size}')
                                                            unify_chunks = False
                                                            
                                                    if unify_chunks:                                                        
                                                        self.logger.info(f'{self.connector.source_id} handle_incoming_connection from '
                                                                        f'peer {self.peername} unifying chunks {files_to_unify} '
                                                                        f'into file {dst_filename_fullpath}')   
                                                        fdunify = open(dst_filename_fullpath, 'wb')
                                                        stderr_data = fallback_manual_cat = None
                                                        try:
                                                            cat_proc = await asyncio.create_subprocess_exec('cat',
                                                                    *files_to_unify, stdout=fdunify, stderr=asyncio.subprocess.PIPE)
                                                            stdout_data, stderr_data = await cat_proc.communicate()
                                                            if stderr_data:
                                                                self.logger.info(f'{self.connector.source_id} Error using cat : '
                                                                             f'{stderr_data[:100]}, fallback to manual cat')                                                                
                                                                fallback_manual_cat = True
                                                        except Exception as excu:
                                                            self.logger.warning(f'{self.connector.source_id} Error unifying : '
                                                                                 f'{excu}, fallback to manual cat')
                                                            fallback_manual_cat = True
                                                            
                                                        if fallback_manual_cat:                                                                                                       
                                                            for thefile in files_to_unify:
                                                                with open(thefile, 'rb') as fdread:
                                                                    ffread = True
                                                                    while ffread:
                                                                        ffread = fdread.read(self.connector.READ_CHUNK_SIZE)
                                                                        fdunify.write(ffread)
                                                                await asyncio.sleep(0)
                                                            ffread = None
                                                        fdunify.close()
                                                    #delete chunks
                                                    for thefile in files_to_unify:
                                                        self.logger.info(f'{self.connector.source_id} handle_incoming_connection from '
                                                                        f'peer {self.peername} deleting chunk {thefile}')
                                                        os.remove(thefile)
                                                    
                                        except Exception:
                                            self.logger.exception(f'{self.connector.source_id} from peer {self.peername} '
                                                                  'chunked exception')
                                            
                                #remove file from binary, whether having written it to dst_fullpath or not. To prevent bloating
                                binary = binary[:binary_offset]
                                if len(binary) == 0:
                                    if MessageFields.WITH_BINARY in transport_json:
                                        del transport_json[MessageFields.WITH_BINARY]
                            except Exception as exc:
                                if isinstance(exc, CustomException):
                                    self.logger.warning(f'{self.connector.source_id} from peer {self.peername} '
                                                      f'handle_incoming_connection with_file : {exc}')
                                else:
                                    self.logger.exception(f'{self.connector.source_id} from peer {self.peername} '
                                                      'handle_incoming_connection with_file')
                                try:
                                    #remove file from binary, whether having written it to dst_fullpath or not. To prevent bloating
                                    binary = binary[:binary_offset]
                                    if len(binary) == 0:
                                        if MessageFields.WITH_BINARY in transport_json:
                                            del transport_json[MessageFields.WITH_BINARY]
                                except Exception:
                                    self.logger.exception(f'{self.connector.source_id} from peer {self.peername} '
                                                      'handle_incoming_connection with_file removal')
                                    
                        else:
                            if dst_dirpath is not False:
                                self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer '
                                                f'{self.peername} tried to create file in non existing directory '
                                                f'{dst_dirpath} for type {with_file.get("dst_type")}, ignoring...')
                            if binary:
                                binary_offset = with_file.get('binary_offset', 0)                            
                                #remove file from binary, whether having written it to dst_fullpath or not. To prevent bloating
                                binary = binary[:binary_offset]
                                if len(binary) == 0:
                                    if MessageFields.WITH_BINARY in transport_json:
                                        del transport_json[MessageFields.WITH_BINARY]
                            
                    
                    #check if this message is a response to an awaiting request, and update put_msg_to_queue_recv
                    response_id = transport_json.get(MessageFields.RESPONSE_ID)                                
                    if response_id is not None:
                        if response_id not in self.connector.messages_awaiting_response[message_type].get(self.peername, {}):
                            self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from peer '
                                                f'{self.peername} got response_id {response_id} not existing in '
                                                f'messages_awaiting_response for type {message_type}. '
                                                'Forwarding to queue_recv anyway...')
                        else:
                            #set the response in case this is the response to a request that came with AWAIT_RESPONSE true
                            self.logger.debug(f'{self.connector.source_id} handle_incoming_connection from peer '
                                              f'{self.peername} got response_id {response_id} in '
                                              f'messages_awaiting_response for type {message_type}')                            
                            self.connector.messages_awaiting_response[message_type][self.peername][response_id][1] = \
                                                                                (transport_json, data, binary)
                            self.connector.messages_awaiting_response[message_type][self.peername][response_id][0].set()
                            put_msg_to_queue_recv = False
                            
                if put_msg_to_queue_recv:                   
                    # send the message to queue
                    self.logger.debug(f'{self.connector.source_id} handle_incoming_connection from '
                                      f'peer {self.peername} putting message to queue_recv')
                    try:
                        self.connector.queue_recv.put_nowait((transport_json, data, binary))
                    except Exception:
                        self.logger.exception('queue_recv.put_nowait')

                transport_json, data, binary = None, None, None
            except asyncio.CancelledError:
                raise     
            except Exception as exc:
                
                self.logger.info('Free response waiters after peer connector disconnection')
                for type_waiters in self.connector.messages_awaiting_response.values():
                    for peer_waiters in type_waiters.values():
                        for response_waiters in peer_waiters.values():
                            #this transport_json error flows to queue_send_to_connector_put (await_response part) and
                            #then generates an IncompleteReadError in api.py (send_message, await_response part)                                                        
                            response_waiters[1] = ({MessageFields.ERROR:'disconnected_during_wait'}, '{}', None)    
                            response_waiters[0].set()
                            
                if isinstance(exc, asyncio.IncompleteReadError):                
                    if self.connector.is_server:                
                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                            f'peer {self.peername} Client disconnected')
                    else:                    
                        self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                            f'peer {self.peername} Server disconnected')
                elif isinstance(exc, ConnectionResetError):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                        f'peer {self.peername} ConnectionResetError : {exc}')
                elif isinstance(exc, ssl.SSLError):
                    self.logger.warning(f'{self.connector.source_id} handle_incoming_connection from '
                                        f'peer {self.peername} SSLError : {exc}')
                else:
                    self.logger.exception(f'{self.connector.source_id} handle_incoming_connection '
                                          f'from peer {self.peername}')
                if not self.is_stopping:
                    if self.connector.disk_persistence:                    
                        self.stop_nowait_for_persistence()      
                    self.stop_task(client_wait_for_reconnect=True)
                return
           
    async def handle_outgoing_connection(self):
        if self.connector.disk_persistence:
            if self.peername not in self.connector.queue_send_transition_to_connect:
                if self.connector.persistence_existence_check(self.peername):
                    self.logger.info(f'{self.connector.source_id} Creating queue_send_transition_to_connect '
                                     f'for peer {self.peername}')
                    self.connector.queue_send_transition_to_connect[self.peername] = asyncio.Queue(maxsize=\
                                                                   self.connector.MAX_QUEUE_SIZE)                
        
        if self.peername in self.connector.queue_send_transition_to_connect:
            #loading persistence messages into queue_send_transition_to_connect
            await self.connector.load_messages_from_persistence(self.peername)            
            queue_send = self.connector.queue_send_transition_to_connect[self.peername]        
            self.logger.info(f'{self.connector.source_id} Entering handle_outgoing_connection_queue for peer '
                             f'{self.peername} with queue_send_transition_to_connect of length {queue_send.qsize()}')
            await self.handle_outgoing_connection_queue(queue_send, lambda :not queue_send.empty())
            del self.connector.queue_send_transition_to_connect[self.peername]
            
        queue_send = self.connector.queue_send[self.peername]
        self.logger.info(f'{self.connector.source_id} Entering handle_outgoing_connection_queue for peer '
                         f'{self.peername} with queue_send of length '+str(queue_send.qsize()))        
        await self.handle_outgoing_connection_queue(queue_send, lambda :True)
        
    async def handle_outgoing_connection_queue(self, queue_send, condition): 
        while condition():
            try:
                self.logger.debug(self.connector.source_id+' handle_outgoing_connection wait for queue_send')     
                if self.connector.SUPPORT_PRIORITIES:
                    priority, the_time, (transport_json, data, binary) = message_tuple = await queue_send.get()
                    message_tuple = message_tuple[2]
                else:
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
                        if len(binary_file) > self.connector.max_size_file_upload_send:
                            raise Exception(f'{self.connector.source_id} cannot send too large file of size {len(binary_file)}')
                    except Exception as exc:
                        self.logger.exception('handle_outgoing_connection handling file : '+str(file_src_path))
                        #del transport_json[MessageFields.WITH_FILE]
                        #send the error msg to peer application, without file
                        transport_json[MessageFields.WITH_FILE]['file_error'] = str(exc)
                    else:
                        if binary_file:
                            #append the file byte content to "binary"
                            if binary:
                                len_binary = len(binary)
                                self.logger.info('handle_outgoing_connection prepare message with both binary and '
                                                 f'binary file at offset {len_binary}')
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
                        self.logger.debug(f'handle_outgoing_connection {self.connector.source_id} send message with '
                                          f'transport_id {self.transport_id} expecting ACK')
                        await self.send_message(transport_json=transport_json, data=data, binary=binary)
                        transport_json, data, binary, message_tuple = None, None, None, None
                        
                        try:
                            await asyncio.wait_for(self.ack_dict[self.transport_id].wait(), timeout=self.connector.send_timeout)
                            self.logger.info(f'handle_outgoing_connection {self.connector.source_id} received ACK for '
                                             f'transport id {self.transport_id}')
                            del self.ack_dict[self.transport_id]
                            break
                        except asyncio.TimeoutError:
                            self.logger.warning(f'handle_outgoing_connection timed out waiting for ACK for '
                                                f'transport id {self.transport_id} at retry {retry}')
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
                        self.connector.full_duplex_connections.pop(self.peername, None)                        
                        if queue_send != self.connector.queue_send.pop(self.peername, None):
                            self.logger.info(f'Special case : disconnection happens during transition. '
                                             f'Transferring {queue_send.qsize()} messages')
                            #we should copy queue_send_transition_to_connect content into a new recreated persistent file                            
                            count = 0
                            disk_persistence_is_list = isinstance(self.connector.disk_persistence, list)
                            while not queue_send.empty():
                                if self.connector.SUPPORT_PRIORITIES:
                                    priority, the_time, (transport_json, data, binary) = queue_send.get_nowait()
                                else:
                                    transport_json, data, binary = queue_send.get_nowait()
                                disk_persistence = True                                
                                if disk_persistence_is_list:
                                    #disk_persistence can be a list of message types
                                    disk_persistence = (transport_json.get(MessageFields.MESSAGE_TYPE) in \
                                                        self.connector.disk_persistence)
                                if disk_persistence:                    
                                    count += 1
                                    message = self.connector.pack_message(transport_json=transport_json,
                                                                          data=data, binary=binary)
                                    self.logger.info(f'Emptying transition queue_send, Storing message '
                                                     f'number {count} to persistence to peername {self.peername}')
                                    self.connector.store_message_to_persistence(self.peername, message, ignore_count=True)                                              
                        else:
                            #regular case of disconnection
                            self.stop_nowait_for_persistence(message_tuple=message_tuple)
                    self.stop_task(client_wait_for_reconnect=True)                    
                return


    #4|2|json|4|data|4|binary
    async def send_message(self, message=None, message_type=None, source_id=None, destination_id=None, request_id=None,
                           response_id=None, transport_json=None, data=None, binary=None, message_type_publish=None):
        await self.send_message_lock.acquire()
        try:
#            async with self.lock_connection:       
            update_msg_counts = True                   
            if message:
                self.logger.debug(self.connector.source_id+' send_message of length {}'.format(len(message)))                         
                if DEBUG_SHOW_DATA:
                    self.logger.info('and with data {}'.format(message))                                                  
            else:
                if transport_json:
                    self.logger.debug(f'{self.connector.source_id} send_message {message_type or transport_json} '
                                      f'with data length {len(data or "")}, and binary length {len(binary or "")}')     
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
                message = self.connector.pack_message(message_type=message_type, source_id=source_id, 
                                                      destination_id=destination_id, request_id=request_id,
                                                      response_id=response_id, transport_json=transport_json,
                                                      data=data, binary=binary, message_type_publish=message_type_publish)
                if DEBUG_SHOW_DATA:
                    self.logger.info('send_message full message ready to send : '+str(message))    

                if transport_json and transport_json[MessageFields.MESSAGE_TYPE] == '_ack':
                    update_msg_counts = False
            # send the length to be sent next
            self.writer.write(message[:Structures.MSG_4_STRUCT.size])
            self.writer.write(message[Structures.MSG_4_STRUCT.size:])    
            try:
                await asyncio.wait_for(self.writer.drain(), timeout=self.connector.send_timeout)
            except asyncio.TimeoutError as exc:
                self.logger.warning('send_message TimeoutError : '+str(exc))
                #requires immediate socket close
                #struct.pack('ii', l_onoff=1, l_linger=0)
                self.connector.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b'\x01\x00\x00\x00\x00\x00\x00\x00')
                #to reproduce : tc qdisc add dev eth0 root netem delay 200ms (to disable : tc qdisc del dev eth0 root)
                self.connector.sock.shutdown(socket.SHUT_RDWR)                                
                raise ConnectionResetError()
                
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
        finally:
            self.send_message_lock.release()
            
            
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
            self.logger.warning(f'{self.connector.source_id} recv_message ConnectionResetError : {exc} with '
                                f'peer {self.peername}')
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
                    
                    if self.client_certificate_id not in self.connector.ssl_helper.default_client_cert_ids_list: 
                        self.logger.warning(f'handle_ssl_messages_server Client '
                    f'{self.connector.ssl_helper.source_id_2_cert["cert_2_source_id"].get(self.client_certificate_id)}'
                    ' tried to get_new_certificate with private certificate. Stopping...')
                        self.stop_task() 
                        return
                    
                    self.logger.info(f'handle_ssl_messages_server receiving get_new_certificate for {self.extra_info}, '
                                     'and calling create_client_certificate')                    
                    #we could have check if client current certificate is default, but is seems limiting, code would be like :
                    #cert_der = self.writer.get_extra_info("ssl_object").getpeercert()
                    #common_name = cert_der["subject"][1][0][1]
                    #if common_name == ssl.DEFAULT_CLIENT_CERTIFICATE_COMMON_NAME:       
                    
                    #Here maybe hook to ask for confirmation before creating a cert for this ip self.extra_info
                
                    if self.connector.whitelisted_clients_id:
                        peername = transport_json[MessageFields.SOURCE_ID]
                        allow_id = False                                        
                        for maybe_regex in self.connector.whitelisted_clients_id:
                            if re.match(maybe_regex, peername):                            
                                self.logger.info(f'{self.connector.source_id} get_new_certificate allowing whitelisted client'
                                                 f' {peername} from ip {self.extra_info}')
                                allow_id = True
                                break
                                
                        if not allow_id:
                            self.logger.info(f'{self.connector.source_id} get_new_certificate blocking non whitelisted '
                                             f'client {peername} from ip {self.extra_info}')
                            
                            if self.connector.hook_whitelist_clients:
                                #here the hook might update some db that might in return send add_whitelist_client
                                #to let this client connect successfully next try (5s)
                                await self.connector.hook_whitelist_clients(self.extra_info, peername)
                            self.stop_task()                                
                            return                    
                            
                    if self.connector.blacklisted_clients_id:
                        peername = transport_json[MessageFields.SOURCE_ID]
                        for maybe_regex in self.connector.blacklisted_clients_id:
                            if re.match(maybe_regex, peername):
                                self.logger.info(f'{self.connector.source_id} get_new_certificate blocking blacklisted'
                                                 f' client {peername} from ip {self.extra_info}')
                                self.stop_task()                                
                                return                    
                    
                    crt_path, key_path = await self.connector.ssl_helper.create_client_certificate(source_id=\
                                            transport_json[MessageFields.SOURCE_ID], common_name=None,
                                            hook_allow_certificate_creation=self.connector.hook_allow_certificate_creation,
                                            server_ca=self.connector.server_ca)
                    with open(crt_path, 'r') as fd:
                        crt = fd.read()
                    with open(key_path, 'r') as fd:
                        key = fd.read()                        
                    response = {'cmd': 'set_new_certificate',
                                'crt': crt,
                                'key': key}
                    if self.connector.server_ca:
                        #no need to keep client certs, which are checked only by their CA
                        os.remove(crt_path)
                        os.remove(key_path)
                    elif self.connector.DELETE_CLIENT_PRIVATE_KEY_ON_SERVER:
                        #we might want to delete now the client private key from server :
                        os.remove(key_path)
                    params_as_string = json.dumps(response)
                    self.logger.info('handle_ssl_messages_server sending set_new_certificate')                
                    await self.send_message(message_type='_ssl', data=params_as_string)
                else:
                    self.logger.warning('handle_ssl_messages_server got invalid command : '+str(data_json.get('cmd')))
                #now server disconnects
                self.stop_task()
            else:                
                if not self.connector.use_token:
                    if data != b'hello':            
                        self.logger.warning(f'Received bad handshake ssl data : {data[:100]}, from client : '
                                            f'{transport_json[MessageFields.SOURCE_ID]}')
                        self.stop_task()
                        return

                old_peername = self.peername
                new_peername = transport_json[MessageFields.SOURCE_ID]
                    
                self.logger.info('Received handshake ssl from client : {}'.format(transport_json[MessageFields.SOURCE_ID]))
                if self.connector.use_token:                
                    message_type = transport_json.get(MessageFields.MESSAGE_TYPE)                                    
                    if message_type != '_token':
                        self.logger.warning(f'{self.connector.source_id} handle_ssl_messages_server : wrong '
                                 f'message_type {message_type[:20]} from client {new_peername} from ip {self.extra_info},'
                                 ' while expecting token')                                                                
                        self.stop_task() 
                        return
                
                validate_source_id(new_peername)
                
                if self.connector.whitelisted_clients_id:
                    allow_id = False                                        
                    for maybe_regex in self.connector.whitelisted_clients_id:
                        if re.match(maybe_regex, new_peername):                            
                            self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server allowing whitelisted client'
                                             f' {new_peername} from ip {self.extra_info}')
                            allow_id = True
                            break
                            
                    if not allow_id:
                        self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server blocking non whitelisted '
                                         f'client {new_peername} from ip {self.extra_info}')
                        
                        if self.connector.hook_whitelist_clients:
                            #here the hook might update some db that might in return send add_whitelist_client
                            #to let this client connect successfully next try (5s)
                            await self.connector.hook_whitelist_clients(self.extra_info, new_peername)
                        self.stop_task()                                
                        return                    
                        
                if self.connector.blacklisted_clients_id:
                    for maybe_regex in self.connector.blacklisted_clients_id:
                        if re.match(maybe_regex, new_peername):
                            self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server blocking blacklisted'
                                             f' client {new_peername} from ip {self.extra_info}')
                            self.stop_task()                                
                            return                
                
                #token mode is supported only with use_ssl and with ssl_allow_all
                if self.connector.use_token:
                    data_json = json.loads(data.decode())                
                    if data_json.get('cmd') == 'get_new_token':
                        if new_peername in self.connector.tokens:
                            self.logger.warning(f'{self.connector.source_id} handle_ssl_messages_server '
                                     f'client {new_peername} from ip {self.extra_info} with existing token '
                                     'sending get_new_token, disconnecting...')
                            self.stop_task() 
                            return
                        if len(self.connector.tokens) > self.connector.MAX_NUMBER_OF_TOKENS:
                            self.logger.warning(f'{self.connector.source_id} handle_ssl_messages_server cannot allocate '
                                     f'token for client {new_peername} from ip {self.extra_info} : too many tokens')                                    
                            self.stop_task() 
                            return
                        new_token = secrets.token_hex(32)
                        self.connector.tokens[new_peername] = self.hash_token(new_token)
                        self.connector.store_server_tokens(self.connector.tokens)
                        response = {'cmd': 'set_new_token', 'token':new_token}
                        params_as_string = json.dumps(response)
                        self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server sending new token to '
                                     f'client {new_peername} from ip {self.extra_info}')    
                        await self.send_message(message_type='_token', data=params_as_string)         
                        self.stop_task()
                        return
                    elif data_json.get('cmd') == 'authenticate':
                        token = data_json.get('token', '')
                        authenticate_success = False
                        if token:
                            hashed = self.hash_token(token[:self.connector.MAX_TOKEN_LENGTH])
                            if hashed == self.connector.tokens.get(new_peername):
                                self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server successfully '
                                         f'authenticated token of client {new_peername} from ip {self.extra_info}')                                    
                                authenticate_success = True
                                
                                response = {'result': 'success'}
                                params_as_string = json.dumps(response)
                                self.logger.info(f'{self.connector.source_id} handle_ssl_messages_server sending token '
                                             f'success to client {new_peername} from ip {self.extra_info}')    
                                await self.send_message(message_type='_token', data=params_as_string)         
                                
                        if not authenticate_success:
                            self.logger.warning(f'{self.connector.source_id} handle_ssl_messages_server : wrong '
                                     f'authentication token from client {new_peername} from ip {self.extra_info}')                                                                
                            self.stop_task() 
                            return
                    else:
                        self.logger.warning(f'{self.connector.source_id} handle_ssl_messages_server : wrong '
                                 f'command {data_json.get("cmd")[:20]} from client {new_peername} from ip {self.extra_info},'
                                 ' while expecting token')                                                                
                        self.stop_task() 
                        return
                
                self.logger.info('Replacing peername {} by {}'.format(old_peername, new_peername))
                self.peername = new_peername                
                self.connector.queue_send[new_peername] = self.connector.queue_send.pop(old_peername)
                self.connector.tasks[self.peername+'_incoming'] = self.connector.tasks.pop(old_peername+'_incoming')

                self.connector.full_duplex_connections[new_peername] = self.connector.full_duplex_connections.pop(old_peername)
                
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
                        key_path = self.connector.ssl_helper.CLIENT_KEY_PATH.format(self.connector.source_id)
                        with open(key_path, 'w') as fd:
                            fd.write(key)
                        os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)

                        #close this connection, and open new connection with newly received certificate
                        self.connector.client_certificate_name = self.connector.source_id
                        #self.stop_task()
                        raise self.TransitionClientCertificateException()
                        
                    else:
                        msg = 'handle_ssl_messages_client got invalid command : '+str(data_json.get('cmd'))
                        self.logger.warning(msg)
                        raise Exception(msg)                     
            else:
                #token mode is supported only with use_ssl and with ssl_allow_all
                if self.connector.use_token:
                    if self.connector.token:
                        self.logger.info('handle_ssl_messages_client sending token authenticate')                                                                
                        await self.send_message(message_type='_token', data=json.dumps({'cmd':'authenticate', \
                                                                                'token':self.connector.token}))
                        transport_json, data, binary = await self.recv_message()
                        if transport_json[MessageFields.MESSAGE_TYPE] != '_token':
                            msg = 'handle_ssl_messages_client received bad message_type : '+str(transport_json)
                            self.logger.warning(msg)
                            raise Exception(msg)                        
                        data_json = json.loads(data.decode())
                        if data_json.get('result') != 'success':
                            msg = 'handle_ssl_messages_client received bad token result : '+str(transport_json)
                            self.logger.warning(msg)
                            raise Exception(msg)
                        self.logger.info('handle_ssl_messages_client received token success') 
                        return
                    else:
                        self.logger.info('handle_ssl_messages_client sending get_new_token')                                        
                        await self.send_message(message_type='_token', data=json.dumps({'cmd':'get_new_token'}))
                        
                        transport_json, data, binary = await self.recv_message()
                        if transport_json[MessageFields.MESSAGE_TYPE] != '_token':
                            msg = 'handle_ssl_messages_client received bad message_type : '+str(transport_json)
                            self.logger.warning(msg)
                            raise Exception(msg)                        
                        data_json = json.loads(data.decode())
                        if data_json.get('cmd') == 'set_new_token':
                            self.logger.info('handle_ssl_messages_client receiving set_new_token')                                          
                            token = data_json.get('token')
                            self.connector.store_client_token(token)
                            #close this connection, and open new connection with newly received token
                            ###self.stop_task()
                            raise self.TransitionClientCertificateException()
                            #self.logger.info('handle_ssl_messages_client sending token authenticate')                                                                
                            #await self.send_message(message_type='_token', data=json.dumps({'cmd':'authenticate',
                            #                                                     'token':self.connector.token}))
                            #return
                            
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
            
    def hash_token(self, token):
        #64bytes to 64bytes
        try:
            res = hashlib.sha256(token.encode()).hexdigest()
            return res
        except Exception:
            self.logger.exception('hash_token')
            return
                    
    async def handle_handshake_no_ssl_server(self, data=None, transport_json=None):
        try:
            if data != b'hello':
                self.logger.warning(f'Received bad handshake_no_ssl data : {data[:100]}, from client : '
                                    f'{transport_json[MessageFields.SOURCE_ID]}')
                self.stop_task()
                return
            
            self.logger.info('Received handshake_no_ssl from client : {}'.format(transport_json[MessageFields.SOURCE_ID]))
            old_peername = self.peername    #str(self.writer.get_extra_info('peername'))
            new_peername = transport_json[MessageFields.SOURCE_ID]
            validate_source_id(new_peername)
            
            if self.connector.whitelisted_clients_id:
                allow_id = False                                        
                for maybe_regex in self.connector.whitelisted_clients_id:
                    if re.match(maybe_regex, new_peername):                            
                        self.logger.info(f'{self.connector.source_id} handle_handshake_no_ssl_server allowing whitelisted client'
                                         f' {new_peername} from ip {self.extra_info}')
                        allow_id = True
                        break
                        
                if not allow_id:
                    self.logger.info(f'{self.connector.source_id} handle_handshake_no_ssl_server blocking non whitelisted '
                                     f'client {new_peername} from ip {self.extra_info}')
                    
                    if self.connector.hook_whitelist_clients:
                        #here the hook might update some db that might in return send add_whitelist_client
                        #to let this client connect successfully next try (5s)
                        await self.connector.hook_whitelist_clients(self.extra_info, new_peername)
                    self.stop_task()                                
                    return                    
                    
            if self.connector.blacklisted_clients_id:
                for maybe_regex in self.connector.blacklisted_clients_id:
                    if re.match(maybe_regex, new_peername):
                        self.logger.info(f'{self.connector.source_id} handle_handshake_no_ssl_server blocking blacklisted'
                                         f' client {new_peername} from ip {self.extra_info}')
                        self.stop_task()                                
                        return                    
            
            self.logger.info('Replacing peername {} by {}'.format(old_peername, new_peername))
            self.peername = new_peername
            #self.logger.info('yomo self.connector.tasks : '+str(self.connector.tasks))
            #self.connector.tasks[new_peername+'_outgoing'] = self.connector.tasks.pop(old_peername+'_outgoing')
            
            self.connector.queue_send[new_peername] = self.connector.queue_send.pop(old_peername)
            self.connector.tasks[self.peername+'_incoming'] = self.connector.tasks.pop(old_peername+'_incoming')
            #if old_peername in self.connector.queue_send_transition_to_connect:
            #    self.connector.queue_send_transition_to_connect[new_peername] = self.connector.queue_send_transition_to_connect.pop(old_peername)
            
            self.connector.full_duplex_connections[new_peername] = self.connector.full_duplex_connections.pop(old_peername)
            
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
            
