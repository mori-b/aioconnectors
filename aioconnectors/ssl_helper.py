import os
import asyncio
import json
import subprocess
import uuid
import shutil

from .helpers import get_tmp_dir

def update_conf(conf_template, conf, replacement_dict):
    shutil.copy(conf_template, conf)
    with open(conf, 'a') as fd:
        for key,value in replacement_dict.items():
            fd.write(str(key)+' = '+str(value)+'\n')
        
class SSL_helper:
    DEFAULT_BASE_PATH = get_tmp_dir() #os.getcwd()
    CLIENT_DEFAULT_CERT_NAME = 'default'    
    SOURCE_ID_2_CERT = 'source_id_2_cert.json'
    CERT_NAME_EXTENSION = "pem"
    KEY_NAME_EXTENSION = "key"
    
    def __init__(self, logger, is_server, certificates_directory_path=None, max_certs=None):
        self.logger = logger.getChild('ssl')
        try:
            self.is_server, self.certificates_directory_path = is_server, certificates_directory_path
            if not self.certificates_directory_path:
                self.certificates_directory_path = self.DEFAULT_BASE_PATH
            self.BASE_PATH = self.certificates_directory_path
            self.certificates_base_path = os.path.join(self.BASE_PATH, 'certificates')
            #server
            self.SERVER_BASE_PATH = os.path.join(self.certificates_base_path, 'server')
            self.DEFAULT_CLIENT_CERTIFICATE_COMMON_NAME = "default.cn.com"
            self.DEFAULT_CLIENT_CERTIFICATE_ORGANIZATION_NAME = "default"    
            self.SERVER_CERTS_PATH  = os.path.join(self.SERVER_BASE_PATH, 'client-certs')
            self.SERVER_CERTS_PEM_PATH  = os.path.join(self.SERVER_CERTS_PATH, '{}.'+self.CERT_NAME_EXTENSION)            
            self.SERVER_CERTS_KEY_PATH  = os.path.join(self.SERVER_CERTS_PATH, '{}.'+self.KEY_NAME_EXTENSION)                        
            self.SERVER_SYMLINKS_PATH = os.path.join(self.SERVER_BASE_PATH, 'client-certs/symlinks')
            self.SERVER_PEM_PATH = os.path.join(self.SERVER_BASE_PATH, 'server-cert/server.'+self.CERT_NAME_EXTENSION)
            self.SERVER_KEY_PATH = os.path.join(self.SERVER_BASE_PATH, 'server-cert/server.'+self.KEY_NAME_EXTENSION)
            self.CSR_CONF  = os.path.join(self.SERVER_BASE_PATH, 'csr_details.conf')
            self.CSR_TEMPLATE_CONF  = os.path.join(self.SERVER_BASE_PATH, 'csr_details_template.conf')
            
            #client
            self.CLIENT_BASE_PATH = os.path.join(self.certificates_base_path, 'client')
            self.CLIENT_PEM_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'client-certs/{}.'+self.CERT_NAME_EXTENSION)
            self.CLIENT_KEY_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'client-certs/{}.'+self.KEY_NAME_EXTENSION)
            #self.CLIENT_DEFAULT_ORGANIZATION = '9d2f849c877b4e50b6fccb54d6cd1818'    #'Internet Widgits Pty Ltd'  #'company' 
            self.CLIENT_SERVER_CRT_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'server-cert/server.crt')                        
            #we might want to chain multiple certificates in CLIENT_SERVER_CRT_PATH, to support multiple server certificates
            
            if self.is_server:
                self.source_id_2_cert_path = os.path.join(self.SERVER_CERTS_PATH, self.SOURCE_ID_2_CERT)
                if os.path.exists(self.source_id_2_cert_path):
                    #load existing source_id_2_cert.json
                    with open(self.source_id_2_cert_path, 'r') as fd:
                        self.source_id_2_cert = json.load(fd)
                else:
                    self.source_id_2_cert = {'source_id_2_cert':{}, 'cert_2_source_id':{}}
                #load default_client_serial
                stdout = subprocess.check_output('openssl x509 -hash -serial -noout -in '+\
                                        str(os.path.join(self.SERVER_CERTS_PATH,
                                            self.CLIENT_DEFAULT_CERT_NAME+'.'+self.CERT_NAME_EXTENSION)), shell=True)
                hash_name, serial = stdout.decode().splitlines()
                self.default_client_serial = serial.split('=')[1]
                self.max_certs = max_certs

        except Exception:
            self.logger.exception('init')
            raise
                    
    async def run_cmd(self, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
    
        stdout, stderr = await proc.communicate()
        return proc, stdout.decode().strip(), stderr    

    '''
    def load_certificate(self, certificate_path):
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend        
        with open(certificate_path, 'rb') as fd:
            content = fd.read()
            certificate = x509.load_pem_x509_certificate(content, default_backend())
    '''
    
    async def create_client_certificate(self, source_id=None, common_name=None):
        #Only called by server
        #Generates self signed certificate for a client_id
        #returns paths of cert and key
        try:
            crt_path = f'{self.SERVER_CERTS_PATH}/{source_id}.{self.CERT_NAME_EXTENSION}'    
            key_path = f'{self.SERVER_CERTS_PATH}/{source_id}.{self.KEY_NAME_EXTENSION}'
            if os.path.exists(crt_path):
                raise Exception(f'A certificate already exists for client {source_id}. '
                                f'Use delete_client_certificate to delete it')
                
            if len(self.source_id_2_cert['source_id_2_cert']) >= self.max_certs:
                raise Exception(f'Too many certificates : {self.max_certs}, failed creating certificate for {source_id}')
                
            if common_name:
                update_conf(self.CSR_TEMPLATE_CONF, self.CSR_CONF, {'O':common_name, 'CN':common_name})
                create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
            else:
                #necessary to set a unique field (like organization), so that each certificate has a unique hash, 
                #which is better for fast authentication
                organization = uuid.uuid4().hex
                update_conf(self.CSR_TEMPLATE_CONF, self.CSR_CONF, {'O':organization})                
                create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
                                     
            proc, stdout, stderr = await self.run_cmd(create_certificate_cmd)
        
            if proc.returncode != 0:
                raise Exception('Error while Generating self signed certificate : '+stderr.decode())
            #if stderr:
            #    self.logger.warning('create_certificate_cmd : '+stderr.decode())
        
            #create symlink for the client certificate named by their fingerprints 
            #so they will be detected by context.load_verify_locations(capath=
            #first, calculate hash for symlink name
            proc, stdout, stderr = await self.run_cmd(f'openssl x509 -hash -serial -noout -in {crt_path}')
            if stderr:
                self.logger.warning('hash : '+stderr.decode())            
            hash_name, serial = stdout.splitlines()
            serial = serial.split('=')[1]
            
            #create a symlink called <hash>.<first free index, starting from 0>, 
            #pointing to f'../{source_id}.{self.CERT_NAME_EXTENSION}'
            index = 0
            while True:
                candidate = os.path.join(self.SERVER_SYMLINKS_PATH, hash_name + '.' + str(index))
                if not os.path.exists(candidate):
                    break
                index += 1
            os.symlink(f'../{source_id}.{self.CERT_NAME_EXTENSION}', candidate)

            
            #backup self.source_id_2_cert in file source_id_2_cert.json
            self.source_id_2_cert['source_id_2_cert'][source_id] = serial
            self.source_id_2_cert['cert_2_source_id'][serial] = source_id            
            with open(self.source_id_2_cert_path, 'w') as fd:
                json.dump(self.source_id_2_cert, fd)
           
            self.logger.info('Generated certificate : '+str(crt_path)+' for : '+source_id)
            return (crt_path, key_path)
        except Exception:
            self.logger.exception('create_client_certificate')
            raise
    
    async def remove_client_cert_on_client(self, source_id):
        try:
            cert_path = self.CLIENT_PEM_PATH.format(source_id)
            key_path = self.CLIENT_KEY_PATH.format(source_id)
            if not os.path.exists(cert_path):
                self.logger.warning(f'remove_client_cert : {source_id} has no certificate to remove at {cert_path}')
            else:                    
                self.logger.info(f'remove_client_cert deleting {cert_path}')
                os.remove(cert_path)                    
                if os.path.exists(key_path):
                    self.logger.info(f'remove_client_cert deleting {key_path}')                        
                    os.remove(key_path)
            return json.dumps({'status':True, 'msg':''})                    
        except Exception as exc:
            self.logger.exception('remove_client_cert_on_client')
            return json.dumps({'status':False, 'msg':str(exc)})

    
    async def remove_client_cert_on_server(self, source_id, remove_only_symlink=False):
        #also remove from self.source_id_2_cert 
        #symlink pointing to f'../{source_id}.{self.CERT_NAME_EXTENSION}' should be removed
        #example : <hash>.2 -> source_id.pem : we need to remove <hash>.2, and then rename all <hash>.i where i>2, to <hash>.i-1 
        try:
            cert_path = self.SERVER_CERTS_PEM_PATH.format(source_id)
            key_path = self.SERVER_CERTS_KEY_PATH.format(source_id)
            
            #first, remove symlink pointing to link_to_find
            link_to_find = f'../{source_id}.{self.CERT_NAME_EXTENSION}'
            
            listdir = os.listdir(self.SERVER_SYMLINKS_PATH)
            for the_file in listdir:
                the_path = os.path.join(self.SERVER_SYMLINKS_PATH, the_file)
                if os.readlink(the_path) == link_to_find:
                    break
            else:
                the_file = None
                self.logger.warning(f'remove_client_cert : could not find a symlink to {link_to_find}')
                                    
            if the_file:
                self.logger.info(f'remove_client_cert deleting {the_path}')
                os.remove(the_path)
                the_file_name, the_file_extension = the_file.split('.')
                the_index = int(the_file_extension)
                #if the_index == 0, nothing more to do
                if the_index:
                    #find and rename others. build "others"= ordered list of indexes of same name as the_file_name
                    others = []
                    for test_file in listdir:
                        test_name, test_index = test_file.split('.')
                        if test_name == the_file_name:
                            if int(test_index) > the_index:
                                others.append(int(test_index))
                    if others:
                        for test_index in sorted(others):
                            os.rename(os.path.join(self.SERVER_SYMLINKS_PATH, the_file_name+'.'+str(test_index)), 
                                      os.path.join(self.SERVER_SYMLINKS_PATH, the_file_name+'.'+str(test_index-1)))                                    
                        
            #then remove paths 
            if not remove_only_symlink:
                if not os.path.exists(cert_path):
                    self.logger.warning(f'remove_client_cert : {source_id} has no certificate to remove at {cert_path}')
                else:                    
                    self.logger.info(f'remove_client_cert deleting {cert_path}')
                    os.remove(cert_path)                    
                    if os.path.exists(key_path):
                        self.logger.info(f'remove_client_cert deleting {key_path}')                        
                        os.remove(key_path)
             
            #also remove from self.source_id_2_cert                 
            cert = self.source_id_2_cert['source_id_2_cert'].pop(source_id, None)
            if cert:
                self.logger.info(f'remove_client_cert removing {source_id} from source_id_2_cert')  
                self.source_id_2_cert['cert_2_source_id'].pop(cert, None)
                with open(self.source_id_2_cert_path, 'w') as fd:
                    json.dump(self.source_id_2_cert, fd)
                return json.dumps({'status':True, 'msg':''})
            else:
                msg = 'remove_client_cert non existing client in source_id_2_cert : '+source_id
                self.logger.error(msg)
                return json.dumps({'status':False, 'msg':msg})
                        
        except Exception as exc:
            self.logger.exception('remove_client_cert_on_server')
            return json.dumps({'status':False, 'msg':str(exc)})

def create_certificates(logger, certificates_directory_path):
    '''
    
    1) Create Server certificate
    openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout server.key -out server.pem -config csr_details.conf
    put in SERVER_PEM_PATH, SERVER_KEY_PATH, CLIENT_SERVER_CRT_PATH (pem renamed to crt)
    
    2) Create client default certificate
    openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout default.key -out default.pem -config csr_details.conf
    ########openssl req -new -newkey rsa -nodes -x509 -days 3650 -subj '/O=9d2f849c877b4e50b6fccb54d6cd1818' -keyout default.key -out default.pem -config csr_details.conf  #'/O=default/CN=default.cn.com'
    put in CLIENT_PEM_PATH, CLIENT_KEY_PATH, SERVER_CERTS_PATH (only pem)
    
    3) Calculate hash of client default certificate
    openssl x509 -hash -noout -in default.pem    #add '.0' as an extension
    
    4) Create symlink of client default certificate in server directory
    ln -s ../default.pem <hash.0>
    
    '''        
    
    ssl_helper = SSL_helper(logger, is_server=False, certificates_directory_path=certificates_directory_path)

    #certificates_path = os.path.join(ssl_helper.BASE_PATH, 'certificates')
    certificates_path = ssl_helper.certificates_base_path
    logger.info('Certificates will be created under directory : '+certificates_path)
        
    certificates_path_server = os.path.join(certificates_path, 'server')
    certificates_path_server_client = os.path.join(certificates_path_server, 'client-certs')       
    certificates_path_server_client_sym = os.path.join(certificates_path_server_client, 'symlinks')               
    certificates_path_server_server = os.path.join(certificates_path_server, 'server-cert')                
    certificates_path_client = os.path.join(certificates_path, 'client')        
    certificates_path_client_client = os.path.join(certificates_path_client, 'client-certs')        
    certificates_path_client_server = os.path.join(certificates_path_client, 'server-cert')                

    if os.path.exists(certificates_path_server_server) and os.listdir(certificates_path_server_server):
        logger.error(certificates_path_server_server+' should be empty before starting this process')
        return False
    if os.path.exists(certificates_path_server_client) and os.listdir(certificates_path_server_client):
        logger.error(certificates_path_server_client+' should be empty before starting this process')
        return False
    if os.path.exists(certificates_path_client) and os.listdir(certificates_path_client):
        logger.error(certificates_path_client+' should be empty before starting this process')
        return False
    
    if not os.path.exists(certificates_path_server_server):
        os.makedirs(certificates_path_server_server)
    if not os.path.exists(certificates_path_server_client_sym):
        os.makedirs(certificates_path_server_client_sym)     
    if not os.path.exists(certificates_path_client_server):
        os.makedirs(certificates_path_client_server)          
    if not os.path.exists(certificates_path_client_client):
        os.makedirs(certificates_path_client_client)                   
        
    SERVER_PEM_PATH = os.path.join(certificates_path_server_server, 'server.pem')
    SERVER_KEY_PATH = os.path.join(certificates_path_server_server, 'server.key')
    CLIENT_SERVER_CRT_PATH = os.path.join(certificates_path_client_server, 'server.crt')
    
    CLIENT_PEM_PATH = os.path.join(certificates_path_client_client, '{}.pem')
    CLIENT_KEY_PATH = os.path.join(certificates_path_client_client, '{}.key')
    SERVER_CERTS_PATH = os.path.join(certificates_path_server_client, '{}.pem')
    
    csr_details_conf = os.path.join(certificates_path_server, 'csr_details.conf')
    csr_details_template_conf = os.path.join(certificates_path_server, 'csr_details_template.conf')
    
    #this if lets the user tweak the base csr_details_template_conf
    if not os.path.exists(csr_details_template_conf):
        with open(csr_details_template_conf, 'w') as fd:
            fd.write(
'''
[req]
prompt = no
default_bits = 2048
default_md = sha256
distinguished_name = dn

[ dn ]
C = US
''')
    else:
        logger.info(f'Using preexisting {csr_details_template_conf}')
        
#        1) Create Server certificate            
    update_conf(csr_details_template_conf, csr_details_conf, {'O':'company'})                        
    cmd = f'openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {SERVER_KEY_PATH} -out {SERVER_PEM_PATH} -config {csr_details_conf}'
    stdout = subprocess.check_output(cmd, shell=True)
    shutil.copy(SERVER_PEM_PATH, CLIENT_SERVER_CRT_PATH)
    #we might want to append to an existing CLIENT_SERVER_CRT_PATH, to support multiple server certificates
 
#        2) Create client default certificate
    client_default_key = CLIENT_KEY_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)
    client_default_pem = CLIENT_PEM_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)
    organization = ssl_helper.CLIENT_DEFAULT_CERT_NAME
    #we might want to obfuscate organization
    organization = str(abs(hash(organization)) % (10 ** 8))
    update_conf(csr_details_template_conf, csr_details_conf, {'O':organization})                    
    cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {client_default_key} -out {client_default_pem} -config {csr_details_conf}"
    stdout = subprocess.check_output(cmd, shell=True)        
    shutil.copy(client_default_pem, SERVER_CERTS_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME))

#        3) Calculate hash of client default certificate
    cmd = f'openssl x509 -hash -noout -in {client_default_pem}'
    stdout = subprocess.check_output(cmd, shell=True)
    the_hash_name = stdout.decode().strip()+'.0'
    
#        4) Create symlink of client default certificate in server directory
    dst = os.path.join(certificates_path_server_client_sym, the_hash_name)
    if os.path.exists(dst):
        os.remove(dst)
    
    cmd = f'ln -s ../{ssl_helper.CLIENT_DEFAULT_CERT_NAME}.pem '+dst
    stdout = subprocess.check_output(cmd, shell=True)
    logger.info('Finished create_certificates')
    return True
            
