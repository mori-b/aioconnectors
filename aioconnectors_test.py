import asyncio
import sys
from datetime import datetime
import os
from copy import deepcopy

import aioconnectors

#This file contains only test examples

'''usage :
First you can create your certificates with the command :
python3 -m aioconnectors create_certificates <optional dir path>
    
Then, open 4 different shells in this order :
python3 aioconnectors_test.py server
python3 aioconnectors_test.py client <client name>
python3 aioconnectors_test.py send2server
python3 aioconnectors_test.py send2client <client name>
'''

SERVER_SOCKADDR = ('127.0.0.1',10673)
CERTIFICATES_DIRECTORY_PATH = None#'/tmp/mo'  #None #default is cwd/connectors/certificates

        
#Here we assume that send_message_types and recv_message_types are based on ['event','command']
results = {}

########################### TEST FLAGS ##############
TEST_WITH_SSL = True
TEST_TRAFFIC_CLIENT = False
TEST_TRAFFIC_SERVER = False
TEST_PERSISTENCE_CLIENT = True
TEST_PERSISTENCE_SERVER = False
TEST_SERVER_AWAITS_REPLY = False
TEST_CLIENT_AWAITS_REPLY = False
TEST_PERSISTENCE_CLIENT_AWAIT_REPLY = False
TEST_MULTIPLE_CLIENTS = True
TEST_UPLOAD_FILE = False
TEST_UPLOAD_FILE_WITH_PERSISTENCE = False
TEST_COMMANDER_SERVER = False    #delete_client_certificate
TEST_COMMANDER_CLIENT = False    #delete_client_certificate
TEST_WITH_ACK = False 
TEST_WITH_SSL_ALLOW_ALL = False


########################### TEST VALUES ##############
UDS_PATH_RECEIVE_PRESERVE_SOCKET = True
UDS_PATH_SEND_PRESERVE_SOCKET = True
DEFAULT_LOGGER_LOG_LEVEL = 'INFO'  #'DEBUG'
SILENT=False
TEST_DEBUG_MSG_COUNTS = True
CLIENT_MESSAGE_TYPES = ['command', 'event']
SERVER_MESSAGE_TYPES = ['event','command']
PERSISTENCE_CLIENT = ['event'] if (TEST_PERSISTENCE_CLIENT or TEST_UPLOAD_FILE_WITH_PERSISTENCE or TEST_PERSISTENCE_CLIENT_AWAIT_REPLY) else False    #True means persistence for both 'event' and 'command'
PERSISTENCE_SERVER = True if TEST_PERSISTENCE_SERVER else False
PERSISTENCE_CLIENT_DELETE_PREVIOUS_PERSISTENCE_FILE = True
PERSISTENCE_SERVER_DELETE_PREVIOUS_PERSISTENCE_FILE = True
CLIENT_NAMES = ['client1','client2'] if TEST_MULTIPLE_CLIENTS else ['client1']
if TEST_UPLOAD_FILE or TEST_UPLOAD_FILE_WITH_PERSISTENCE:
    FILE_TYPE2DIRPATH = {'pcap':'/tmp/pcap', 'binary':'/tmp/binary'}    #{}
    FILE_SRC_PATH = '/tmp/file_src'    #''
    
    if FILE_TYPE2DIRPATH:
        for dir_path in FILE_TYPE2DIRPATH.values():
            if not os.path.exists(dir_path):
                print('Creating directory '+dir_path+' for FILE_TYPE2DIRPATH')
                os.makedirs(dir_path)
    if FILE_SRC_PATH:
        if not os.path.exists(FILE_SRC_PATH):
            with open(FILE_SRC_PATH, 'w') as fd:
                fd.write('Test upload file')
else:
    FILE_TYPE2DIRPATH = None
                
########################### END TEST ##############

async def print_results(interval=2):
    #shows source_id:message_type:peer_id:sendORrecv
   # start = time.time()
    while True:
        await asyncio.sleep(interval)
        print(str(datetime.now()))
        print(results)
        #if str(results).count('300')>=2:
        #    end = time.now()
        #    print('Duration in seconds : '+str(end-start))


def increment_result(source_id, peer_id, message_type, sendORrecv):
    if source_id not in results:
        results[source_id] = {}
    if message_type not in results[source_id]:
        results[source_id][message_type] = {}
    if peer_id not in results[source_id][message_type]:
        results[source_id][message_type][peer_id] = {'send':0, 'recv':0}
    #if sendORrecv not in results[name][event_type][source_id]:
    #    results[source_id][event_type][peer_id][sendORrecv] = 0
    results[source_id][message_type][peer_id][sendORrecv] +=1
    

if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(sys.argv)
        local_name = None
        if len(sys.argv) > 2:
            local_name = sys.argv[2]    #currently only for client
            
        if sys.argv[1] == 'server':
            print('Started server')
                        
            connector_manager = aioconnectors.ConnectorManager(config_file_path=None, default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL, is_server=True, server_sockaddr=SERVER_SOCKADDR, use_ssl=TEST_WITH_SSL, certificates_directory_path=CERTIFICATES_DIRECTORY_PATH,
                                                    disk_persistence_send=PERSISTENCE_SERVER, disk_persistence_recv=PERSISTENCE_SERVER, debug_msg_counts=TEST_DEBUG_MSG_COUNTS, silent=SILENT, #use_ack=TEST_WITH_ACK,
                                                    send_message_types=SERVER_MESSAGE_TYPES, recv_message_types=CLIENT_MESSAGE_TYPES, file_type2dirpath=FILE_TYPE2DIRPATH,
                                                    uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET, ssl_allow_all=TEST_WITH_SSL_ALLOW_ALL)
            loop = asyncio.get_event_loop()
            
            if PERSISTENCE_CLIENT_DELETE_PREVIOUS_PERSISTENCE_FILE:
                connector_manager.delete_previous_persistence_remains()
                            
            loop.create_task(connector_manager.start_connector())
            #loop.create_task(connector_manager.stop_connector(delay=10, hard=True))          
            
            if TEST_PERSISTENCE_CLIENT:
                loop.create_task(connector_manager.restart_connector(delay=15, sleep_between=3, hard=True))
            if TEST_UPLOAD_FILE_WITH_PERSISTENCE:
                loop.create_task(connector_manager.restart_connector(delay=15, sleep_between=4, hard=True))
            if TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                loop.create_task(connector_manager.restart_connector(delay=12, sleep_between=2, hard=True))
                
            try:
                loop.run_forever()
            except:
                task_stop = loop.create_task(connector_manager.stop_connector(shutdown=True))            
                loop.run_until_complete(task_stop)
                del connector_manager                
                print('Server stopped !')
                
        elif sys.argv[1] == 'client':
            print('Started client')
            if TEST_PERSISTENCE_CLIENT or TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                disk_persistence = ['event']
            connector_manager = aioconnectors.ConnectorManager(default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL, is_server=False, server_sockaddr=SERVER_SOCKADDR, use_ssl=TEST_WITH_SSL, certificates_directory_path=CERTIFICATES_DIRECTORY_PATH, 
                                                    client_name=local_name, disk_persistence_send=PERSISTENCE_CLIENT, disk_persistence_recv=PERSISTENCE_CLIENT, debug_msg_counts=TEST_DEBUG_MSG_COUNTS, silent=SILENT, #use_ack=TEST_WITH_ACK,
                                                    send_message_types=CLIENT_MESSAGE_TYPES, recv_message_types=SERVER_MESSAGE_TYPES,
                                                    uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET, uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET, ssl_allow_all=TEST_WITH_SSL_ALLOW_ALL)
            loop = asyncio.get_event_loop()

            if PERSISTENCE_CLIENT_DELETE_PREVIOUS_PERSISTENCE_FILE:
                connector_manager.delete_previous_persistence_remains()            
                
            loop.create_task(connector_manager.start_connector())
            #loop.create_task(connector_manager.stop_connector(delay=7, hard=True))          
            
            #loop.create_task(connector_manager.restart_connector(delay=20, sleep_between=3))
            if TEST_PERSISTENCE_SERVER:
                #loop.create_task(connector_manager.restart_connector(delay=7, sleep_between=5))
                loop.create_task(connector_manager.restart_connector(delay=16, sleep_between=2, hard=True))
                
            
            try:
                loop.run_forever()
            except:
                task_stop = loop.create_task(connector_manager.stop_connector(shutdown=True))            
                loop.run_until_complete(task_stop)
                del connector_manager                         
                print('Client stopped !')

            
        elif sys.argv[1] == 'send2client':
            print('Started send2client')
            own_source_id = local_name or CLIENT_NAMES[0]
            
            connector_api = aioconnectors.ConnectorAPI(default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL, server_sockaddr=SERVER_SOCKADDR, client_name=own_source_id, is_server=False,
                                                       send_message_types=CLIENT_MESSAGE_TYPES, recv_message_types=SERVER_MESSAGE_TYPES,
                                                       uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET)#, uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET)
            loop = asyncio.get_event_loop()
                            
            if TEST_COMMANDER_CLIENT:
                loop.create_task(connector_api.delete_client_certificate())
            
            async def client_cb_event(transport_json , data, binary):
                peer_id = transport_json['source_id']                                    
                increment_result(own_source_id, peer_id, 'event', 'recv')

                
            async def client_cb_command(transport_json , data, binary):
                peer_id = transport_json['source_id']   
                increment_result(own_source_id, peer_id, 'command', 'recv')

                
            loop.create_task(print_results())
            #wait for messages from server (call once only)   
            if True: #TEST_PERSISTENCE_CLIENT or TEST_PERSISTENCE_SERVER or TEST_SERVER_AWAITS_REPLY or TEST_CLIENT_AWAITS_REPLY or TEST_UPLOAD_FILE:
                loop.create_task(connector_api.start_waiting_for_messages(message_type='command', message_received_cb=client_cb_command))
                #loop.create_task(connector_api.start_waiting_for_messages(message_type='event', message_received_cb=client_cb_command))
                
            async def send_stress(message_type, peer_id, delay):
                await asyncio.sleep(delay)    

                index = 0       
                await_response = False 
                with_file_template = False
                with_file = None
                
                if TEST_PERSISTENCE_CLIENT:                    
                    duration_test = 20 #seconds
                    messages_per_second = 1000                    
                    if TEST_WITH_ACK:                    
                        messages_per_second = 10
                elif (TEST_SERVER_AWAITS_REPLY or TEST_CLIENT_AWAITS_REPLY):
                    duration_test = 10 #seconds
                    messages_per_second = 10
                    if TEST_CLIENT_AWAITS_REPLY:
                        await_response = True
                elif TEST_UPLOAD_FILE or TEST_UPLOAD_FILE_WITH_PERSISTENCE:
                    duration_test = 10 #seconds
                    if TEST_UPLOAD_FILE_WITH_PERSISTENCE:
                        duration_test = 20
                    messages_per_second = 1
                    with_file_template={'src_path':FILE_SRC_PATH,'dst_type':'pcap', 'dst_name':os.path.basename(FILE_SRC_PATH)+'_from_client_'+own_source_id+'_index_{}', 'delete':False} #default is delete=True
                elif TEST_TRAFFIC_CLIENT:
                    duration_test = 10 #seconds
                    messages_per_second = 1000
                    if TEST_WITH_ACK:
                        messages_per_second = 10
                elif TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                    duration_test = 20 #seconds
                    messages_per_second = 2
                    await_response = True
                        
                max_index = duration_test * messages_per_second
                sleep_time = 1/messages_per_second
                request_id = response_id = None
                while index < max_index:
                    index += 1
                    data = 'טסט(% ;)'+str(index)*200  
                    #increment_result(own_source_id, peer_id, message_type, 'send')
                    #while results[own_source_id][message_type][peer_id]['recv'] != index:
                    #    await asyncio.sleep(0.1)
                    if TEST_SERVER_AWAITS_REPLY:
                        response_id = index
                    else:
                        request_id = index
                    if with_file_template:
                        with_file = deepcopy(with_file_template)
                        with_file['dst_name'] = with_file['dst_name'].format(index)
                    response = await connector_api.send_message(data=data, data_is_json=False, 
                                                        message_type=message_type, await_response=await_response, response_id=response_id, request_id=request_id,
                                                        binary=b'\x01\x02\x03\x04\x05', with_file=with_file, wait_for_ack=TEST_WITH_ACK)
                    increment_result(own_source_id, peer_id, message_type, 'send')                                                            
                    #await asyncio.sleep(sleep_time)
                    
                    if TEST_CLIENT_AWAITS_REPLY or TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                        increment_result(own_source_id, peer_id, message_type, 'recv')
                    else:                                    
                        await asyncio.sleep(sleep_time)                        
                    
                print('Finished')
                    
                    
            if TEST_PERSISTENCE_CLIENT or TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:                                        
                loop.create_task(send_stress(message_type='event', peer_id=str(SERVER_SOCKADDR), delay=2))
            elif TEST_SERVER_AWAITS_REPLY:    
                loop.create_task(send_stress(message_type='command', peer_id=str(SERVER_SOCKADDR), delay=7))
            elif TEST_CLIENT_AWAITS_REPLY:
                loop.create_task(send_stress(message_type='command', peer_id=str(SERVER_SOCKADDR), delay=3))
            elif TEST_UPLOAD_FILE or TEST_UPLOAD_FILE_WITH_PERSISTENCE:
                loop.create_task(send_stress(message_type='event', peer_id=str(SERVER_SOCKADDR), delay=3))
            elif TEST_TRAFFIC_CLIENT:
                loop.create_task(send_stress(message_type='event', peer_id=str(SERVER_SOCKADDR), delay=2))
                
            try:
                loop.run_forever()
            except:
                print('send2client stopped !')
                #for task in tasks:
                #    task.cancel()
                
        elif sys.argv[1] == 'send2server':
            print('Started send2server')          
            own_source_id = str(SERVER_SOCKADDR)            
            #name = local_name or str(SERVER_SOCKADDR)
            #server_source_id = str(SERVER_SOCKADDR)
            
            connector_api = aioconnectors.ConnectorAPI(default_logger_log_level=DEFAULT_LOGGER_LOG_LEVEL, is_server=True, server_sockaddr=SERVER_SOCKADDR,
                                                send_message_types=SERVER_MESSAGE_TYPES, recv_message_types=CLIENT_MESSAGE_TYPES,
                                                uds_path_send_preserve_socket=UDS_PATH_SEND_PRESERVE_SOCKET)#, uds_path_receive_preserve_socket=UDS_PATH_RECEIVE_PRESERVE_SOCKET)
#            connector_api = aioconnectors.ConnectorAPI(is_server=True, server_sockaddr=own_source_id, use_ssl=TEST_WITH_SSL, certificates_directory_path=CERTIFICATES_DIRECTORY_PATH,

            
            loop = asyncio.get_event_loop()

            if TEST_COMMANDER_SERVER:
                loop.create_task(connector_api.delete_client_certificate(client_id='client2', remove_only_symlink=False))
            '''    
            async def print_queues(period):
                while True:
                    res = await connector_api.peek_queues()
                    await asyncio.sleep(period)
                    print(res)                
            loop.create_task(print_queues(3))
            '''
                
            async def server_cb_event(transport_json , data, binary):
                peer_id = transport_json['source_id']                                    
                increment_result(own_source_id, peer_id, 'event', 'recv')

                
            async def server_cb_command(transport_json , data, binary):
                peer_id = transport_json['source_id']                    
                increment_result(own_source_id, peer_id, 'command', 'recv')


            loop.create_task(print_results())
            #wait for messages from client (call once only)     
            if True: #TEST_PERSISTENCE_CLIENT or TEST_PERSISTENCE_SERVER or TEST_CLIENT_AWAITS_REPLY or TEST_UPLOAD_FILE:                   
                loop.create_task(connector_api.start_waiting_for_messages(message_type='event', message_received_cb=server_cb_event))
                loop.create_task(connector_api.start_waiting_for_messages(message_type='command', message_received_cb=server_cb_command))
                        
            async def send_stress(message_type, peer_id, delay=0):
                index = 0        
                await_response = False
                with_file_template = False
                with_file = None
                
                if TEST_PERSISTENCE_SERVER:                
                    duration_test = 20 #seconds
                    messages_per_second = 1000 #1000
                    if TEST_WITH_ACK:                    
                        messages_per_second = 10                    
                elif TEST_SERVER_AWAITS_REPLY or TEST_CLIENT_AWAITS_REPLY:
                    duration_test = 10 #seconds
                    messages_per_second = 10 #1000
                    if TEST_SERVER_AWAITS_REPLY:
                        await_response = True
                elif TEST_UPLOAD_FILE:
                    duration_test = 10 #seconds
                    messages_per_second = 1
                    with_file_template={'src_path':FILE_SRC_PATH,'dst_type':'binary', 'dst_name':os.path.basename(FILE_SRC_PATH)+'_to_client_'+peer_id+'_index_{}', 'delete':False} #default is delete=True
                elif TEST_TRAFFIC_SERVER:                
                    duration_test = 20 #seconds
                    messages_per_second = 1000 #1000
                    if TEST_WITH_ACK:
                        messages_per_second = 10                   
                elif TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                    duration_test = 20 #seconds
                    messages_per_second = 2
                    
                max_index = duration_test * messages_per_second
                sleep_time = 1/messages_per_second

                await asyncio.sleep(delay)    #let the server start, then clients
                request_id = response_id = None
                while index < max_index:
                    index += 1                    
                    data = str(index)*200      
                    if TEST_CLIENT_AWAITS_REPLY or TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                        response_id = index
                    else:
                        request_id = index     
                    if with_file_template:
                        with_file = deepcopy(with_file_template)
                        with_file['dst_name'] = with_file['dst_name'].format(index)
                        
                    response = await connector_api.send_message(data=data, data_is_json=False, destination_id=peer_id,
                                                        message_type=message_type, await_response=await_response, request_id=request_id,
                                                        response_id=response_id, binary=b'\x01\x02\x03\x04\x05', with_file=with_file, wait_for_ack=TEST_WITH_ACK)

                    increment_result(own_source_id, peer_id, message_type, 'send')  

                    if TEST_SERVER_AWAITS_REPLY:
                        increment_result(own_source_id, peer_id, message_type, 'recv')
                    else:                                    
                        await asyncio.sleep(sleep_time)
                        
                print('Finished')

            if TEST_PERSISTENCE_SERVER:                                                    
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='command', peer_id=client_id, delay=5))#5))
            elif TEST_SERVER_AWAITS_REPLY:
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='command', peer_id=client_id, delay=4))#5))
            elif TEST_CLIENT_AWAITS_REPLY:
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='command', peer_id=client_id, delay=8))#5))
            elif TEST_UPLOAD_FILE:
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='command', peer_id=client_id, delay=5))#5))                    
            elif TEST_TRAFFIC_SERVER:                                                    
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='command', peer_id=client_id, delay=5))#5))
            elif TEST_PERSISTENCE_CLIENT_AWAIT_REPLY:
                for client_id in CLIENT_NAMES:
                    loop.create_task(send_stress(message_type='event', peer_id=client_id, delay=6))#5))
                    
                    
            
            
            try:
                loop.run_forever()
            except:
                print('send2server stopped !')
      
      #          for task in tasks:
      #              task.cancel()        
                    

        else:
            print('Bad input',sys.argv)
    else:
        print('Bad Parameters',sys.argv)
