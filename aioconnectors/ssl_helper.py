import os
import asyncio
import json
import subprocess
import uuid
import shutil
import re

from .helpers import get_tmp_dir, validate_source_id, SOURCE_ID_DEFAULT_REGEX

CA_CREATE_CNF ='''
[ ca ]
default_ca    = CA_default      # The default ca section

[ CA_default ]

default_days     = 3650         # How long to certify for
default_crl_days = 3650         # How long before next CRL
default_md       = sha256       # Use public key default MD
preserve         = no           # Keep passed DN ordering

x509_extensions = ca_extensions # The extensions to add to the cert

email_in_dn     = no            # Don't concat the email in the DN
copy_extensions = copy          # Required to copy SANs from CSR to cert

base_dir      = {base_dir}
certificate   = $base_dir/{ca_pem}   # The CA certificate
private_key   = $base_dir/{ca_key}    # The CA private key
new_certs_dir = {ca_generated}            # Location for new certs after signing
database      = $base_dir/index.txt    # Database index file
serial        = $base_dir/serial.txt   # The current serial number

unique_subject = no  # Set to 'no' to allow creation of
                     # several certificates with same subject.

[ req ]
prompt             = no
default_bits       = 4096
default_keyfile    = server_ca.pem
distinguished_name = ca_distinguished_name
x509_extensions    = ca_extensions
string_mask        = utf8only

[ ca_distinguished_name ]

countryName                    = US
#countryName_default            = US


[ ca_extensions ]

subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always, issuer
basicConstraints       = critical, CA:true
keyUsage               = keyCertSign, cRLSign

[ signing_policy ]
countryName            = optional
stateOrProvinceName    = optional
localityName           = optional
organizationName       = supplied
organizationalUnitName = optional
commonName             = optional
emailAddress           = optional

[ signing_req ]
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer
basicConstraints       = CA:FALSE
keyUsage               = digitalSignature, keyEncipherment
'''

CA_CSR_CREATE_CNF = '''
[ req ]
prompt             = no
default_bits       = 2048
default_keyfile    = serverkey.pem
distinguished_name = server_distinguished_name
req_extensions     = server_req_extensions
string_mask        = utf8only

[ server_distinguished_name ]
countryName                 = US
organizationName               = {org}

[ server_req_extensions ]
subjectKeyIdentifier = hash
basicConstraints     = CA:FALSE
keyUsage             = digitalSignature, keyEncipherment
'''

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
    
    def __init__(self, logger, is_server, certificates_directory_path=None, max_certs=None, server_ca=False,
                 server_ca_certs_not_stored=True, tool_only=False):
        self.logger = logger.getChild('ssl')
        try:
            self.is_server, self.certificates_directory_path, self.server_ca = is_server, certificates_directory_path, server_ca
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
            self.SERVER_CA_DETAILS_CONF = os.path.join(self.SERVER_BASE_PATH, 'server_ca_details.conf')            
            self.CSR_TEMPLATE_CONF  = os.path.join(self.SERVER_BASE_PATH, 'csr_details_template.conf')
            self.SERVER_CA_PEM_PATH = os.path.join(self.SERVER_BASE_PATH, 'server-cert/server_ca.'+self.CERT_NAME_EXTENSION)
            self.SERVER_CA_KEY_PATH = os.path.join(self.SERVER_BASE_PATH, 'server-cert/server_ca.'+self.KEY_NAME_EXTENSION)
            self.SERVER_CA_CSR_CONF  = os.path.join(self.SERVER_BASE_PATH, 'server_ca_csr_details.conf')
            self.SERVER_CA_CSR_PEM_PATH = os.path.join(self.SERVER_BASE_PATH, 'server-cert/server_ca_csr.'+self.CERT_NAME_EXTENSION)
            self.CA_GENERATED = os.path.join(self.SERVER_CERTS_PATH, 'ca-generated')
            
            #client
            self.CLIENT_BASE_PATH = os.path.join(self.certificates_base_path, 'client')
            self.CLIENT_PEM_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'client-certs/{}.'+self.CERT_NAME_EXTENSION)
            self.CLIENT_KEY_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'client-certs/{}.'+self.KEY_NAME_EXTENSION)
            #self.CLIENT_DEFAULT_ORGANIZATION = '9d2f849c877b4e50b6fccb54d6cd1818'    #'Internet Widgits Pty Ltd'  #'company' 
            self.CLIENT_SERVER_CRT_PATH  = os.path.join(self.CLIENT_BASE_PATH, 'server-cert/server.'+self.CERT_NAME_EXTENSION)                        
            #we might want to chain multiple certificates in CLIENT_SERVER_CRT_PATH, to support multiple server certificates
            self.tool_only = tool_only
            if self.tool_only:
                return
            
            if self.is_server:
                self.source_id_2_cert_path = os.path.join(self.SERVER_CERTS_PATH, self.SOURCE_ID_2_CERT)
                if os.path.exists(self.source_id_2_cert_path):
                    #load existing source_id_2_cert.json
                    with open(self.source_id_2_cert_path, 'r') as fd:
                        self.source_id_2_cert = json.load(fd)
                else:
                    self.source_id_2_cert = {'source_id_2_cert':{}, 'cert_2_source_id':{}}
                #load default_client_cert_id, and default_client_cert_ids_list
                self.default_client_cert_ids_list = []
                for cert in (file_name for file_name in os.listdir(self.SERVER_CERTS_PATH) if \
                                                     file_name.endswith(f'.{self.CERT_NAME_EXTENSION}')):
                    if cert.startswith(self.CLIENT_DEFAULT_CERT_NAME):
                        cert_name = cert[:-1-len(self.CERT_NAME_EXTENSION)]
                        if SOURCE_ID_DEFAULT_REGEX.match(cert_name):
                            stdout = subprocess.check_output('openssl x509 -hash -serial -noout -in '+\
                                                    str(os.path.join(self.SERVER_CERTS_PATH,
#                                                        self.CLIENT_DEFAULT_CERT_NAME+'.'+self.CERT_NAME_EXTENSION)), shell=True)
                                                        cert)), shell=True)
                            
                            hash_name, serial = stdout.decode().splitlines()
                            serial = serial.split('=')[1]
                            if cert_name == self.CLIENT_DEFAULT_CERT_NAME:
                                self.default_client_cert_id = serial
                            self.logger.info(f'Server adding default certificate : {cert_name}')
                            self.default_client_cert_ids_list.append(serial)
                self.logger.info(f'Server using default_client_cert_ids_list : {self.default_client_cert_ids_list}')                 
                self.max_certs = max_certs
                self.server_ca_certs_not_stored = server_ca_certs_not_stored

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
    
    async def create_client_certificate(self, source_id=None, common_name=None, hook_allow_certificate_creation=None,
                                        server_ca=False):
        #Only called by server
        #Generates self signed certificate for a client_id
        #returns paths of cert and key
        try:
            validate_source_id(source_id)
            crt_path = f'{self.SERVER_CERTS_PATH}/{source_id}.{self.CERT_NAME_EXTENSION}'    
            key_path = f'{self.SERVER_CERTS_PATH}/{source_id}.{self.KEY_NAME_EXTENSION}'
            if os.path.exists(crt_path):
                raise Exception(f'A certificate already exists for client {source_id}. '
                                f'Use delete_client_certificate to delete it')

            if source_id in self.source_id_2_cert['source_id_2_cert']:
                raise Exception(f'A source_id_2_cert already exists for client {source_id}. '
                            f'Use delete_client_certificate to delete it')                    
                
            if hook_allow_certificate_creation:
                allow_certificate_creation = await hook_allow_certificate_creation(source_id)
                if not allow_certificate_creation:
                    raise Exception(f'Not allowing certificate creation for {source_id}')
                    
            if len(self.source_id_2_cert['source_id_2_cert']) >= self.max_certs:
                raise Exception(f'Too many certificates : {self.max_certs}, failed creating certificate for {source_id}')
                
            if common_name:
                update_conf(self.CSR_TEMPLATE_CONF, self.CSR_CONF, {'O':common_name, 'CN':common_name})
                create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
                raise Exception('Need to implement create_client_certificate with common_name')
            else:
                #necessary to set a unique field (like organization), so that each certificate has a unique hash, 
                #which is better for fast authentication
                organization = uuid.uuid4().hex
                update_conf(self.CSR_TEMPLATE_CONF, self.CSR_CONF, {'O':organization})
                if not server_ca:
                    create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
                    proc, stdout, stderr = await self.run_cmd(create_certificate_cmd)                
                    if proc.returncode != 0:
                        raise Exception('Error while Generating self signed certificate : '+stderr.decode())
                                         
                else:
                    #create csr and sign it with server_ca
                    with open(self.SERVER_CA_CSR_CONF, 'w') as fd:
                        fd.write(CA_CSR_CREATE_CNF.format(org=organization))
                    #update_conf(SERVER_CA_CSR_CONF, csr_details_conf, {'O':organization})            
                    csr_path = f'{self.SERVER_CERTS_PATH}/{source_id}-csr.{self.CERT_NAME_EXTENSION}'    
                    
                    create_csr_cmd = f"openssl req -config {self.SERVER_CA_CSR_CONF} -newkey rsa:2048 -sha256 -nodes -keyout "\
                                    f"{key_path} -out {csr_path} -outform PEM"
                    proc, stdout, stderr = await self.run_cmd(create_csr_cmd)                
                    if proc.returncode != 0:
                        raise Exception('Error while Generating csr : '+stderr.decode())
                                    
                    self.logger.info('Sign client default certificate CSR')
                    pem_path = f'{self.SERVER_CERTS_PATH}/{source_id}.{self.CERT_NAME_EXTENSION}'    
                    
                    # Create the index file
                    index_file_path = os.path.join(self.SERVER_BASE_PATH, 'index.txt')                    
                    if not os.path.exists(index_file_path):
                        open(index_file_path, 'w').close()

                    create_certificate_cmd = f"openssl ca -rand_serial -batch -policy signing_policy -config {self.SERVER_CA_DETAILS_CONF} "\
                                            f"-extensions signing_req -out {pem_path} -infiles {csr_path}"
                    stdout = subprocess.check_output(create_certificate_cmd, shell=True)       
                    proc, stdout, stderr = await self.run_cmd(create_certificate_cmd)                
                    if proc.returncode != 0:
                        raise Exception('Error while Generating CA signed certificate : '+stderr.decode())
                        
                    if self.server_ca_certs_not_stored:
                        try:
                            for cert in os.listdir(self.CA_GENERATED):
                                path_to_delete = os.path.join(self.CA_GENERATED, cert)
                                self.logger.info(f'Deleting {path_to_delete}')
                                os.remove(path_to_delete)
                        except Exception:
                            self.logger.exception(f'Deleting {csr_path}')
                        
                    try:
                        if os.path.exists(csr_path):
                            #deleting client csr
                            os.remove(csr_path)
                    except Exception:
                        self.logger.exception(f'Deleting {csr_path}')
                                     
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
            cert_id = serial

            if not server_ca:
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
            self.source_id_2_cert['source_id_2_cert'][source_id] = cert_id
            self.source_id_2_cert['cert_2_source_id'][cert_id] = source_id            
            with open(self.source_id_2_cert_path, 'w') as fd:
                json.dump(self.source_id_2_cert, fd)
           
            self.logger.info('Generated certificate : '+str(crt_path)+' for : '+source_id)
            return (crt_path, key_path)
        except Exception:
            self.logger.exception('create_client_certificate')
            raise
    
    async def remove_client_cert_on_client(self, source_id):
        try:
            validate_source_id(source_id)
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
            validate_source_id(source_id)
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

def create_certificates(logger, certificates_directory_path, no_ca=True):
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
    
    5) Create Server CA certificate
                                
    '''        
    
    ssl_helper = SSL_helper(logger, is_server=False, certificates_directory_path=certificates_directory_path)

    #certificates_path = os.path.join(ssl_helper.BASE_PATH, 'certificates')
    certificates_path = ssl_helper.certificates_base_path
    logger.info('Certificates will be created under directory : '+certificates_path)
        
    certificates_path_server = os.path.join(certificates_path, 'server')
    certificates_path_server_client = os.path.join(certificates_path_server, 'client-certs')
    certificates_path_server_client_gen = os.path.join(certificates_path_server_client, 'ca-generated')     
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
    if not no_ca:
        if not os.path.exists(certificates_path_server_client_gen):
            os.makedirs(certificates_path_server_client_gen)             

    CERT_NAME_EXTENSION = "pem"
    KEY_NAME_EXTENSION = "key"

    #server
    SERVER_CERTS_PEM_PATH = os.path.join(certificates_path_server_client, '{}.'+CERT_NAME_EXTENSION)
    SERVER_PEM_PATH = os.path.join(certificates_path_server_server, 'server.'+CERT_NAME_EXTENSION)
    SERVER_KEY_PATH = os.path.join(certificates_path_server_server, 'server.'+KEY_NAME_EXTENSION)
    SERVER_CA_PEM = 'server_ca.pem'
    SERVER_CA_KEY = 'server_ca.key'
    SERVER_CA_PEM_PATH = os.path.join(certificates_path_server_server, SERVER_CA_PEM)
    SERVER_CA_KEY_PATH = os.path.join(certificates_path_server_server, SERVER_CA_KEY)
    SERVER_CA_CSR_CONF  = os.path.join(certificates_path_server, 'server_ca_csr_details.conf')
    SERVER_CA_CSR_PEM_PATH = os.path.join(certificates_path_server, 'server-cert/server_ca_csr.'+CERT_NAME_EXTENSION)
    
    #client
    CLIENT_SERVER_CRT_PATH = os.path.join(certificates_path_client_server, 'server.'+CERT_NAME_EXTENSION)
    CLIENT_PEM_PATH = os.path.join(certificates_path_client_client, '{}.'+CERT_NAME_EXTENSION)
    CLIENT_KEY_PATH = os.path.join(certificates_path_client_client, '{}.'+KEY_NAME_EXTENSION)
    
    SERVER_CA_DETAILS_CONF = os.path.join(certificates_path_server, 'server_ca_details.conf')    
    CSR_CONF  = os.path.join(certificates_path_server, 'csr_details.conf')    
    CSR_TEMPLATE_CONF  = os.path.join(certificates_path_server, 'csr_details_template.conf')    
    
    #this if lets the user tweak the base CSR_TEMPLATE_CONF
    if not os.path.exists(CSR_TEMPLATE_CONF):
        with open(CSR_TEMPLATE_CONF, 'w') as fd:
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
        logger.info(f'Using preexisting {CSR_TEMPLATE_CONF}')

    logger.info('Create Server certificate')
#        1) Create Server certificate            
    update_conf(CSR_TEMPLATE_CONF, CSR_CONF, {'O':'company'})                        
    cmd = f'openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {SERVER_KEY_PATH} -out {SERVER_PEM_PATH} -config {CSR_CONF}'
    stdout = subprocess.check_output(cmd, shell=True)
    shutil.copy(SERVER_PEM_PATH, CLIENT_SERVER_CRT_PATH)
    #we might want to append to an existing CLIENT_SERVER_CRT_PATH, to support multiple server certificates

    if not no_ca:
        logger.info('Generate CA')
    #        2)generate ca pem and key
        if not os.path.exists(SERVER_CA_DETAILS_CONF):
            with open(SERVER_CA_DETAILS_CONF, 'w') as fd:
                fd.write(CA_CREATE_CNF.format(base_dir=certificates_path_server_server, ca_generated=certificates_path_server_client_gen,
                                              ca_pem=SERVER_CA_PEM, ca_key=SERVER_CA_KEY))
              
        cmd = f"openssl req -new -newkey rsa:4096 -sha256 -nodes -x509 -days 3650 -keyout {SERVER_CA_KEY_PATH}" \
              f" -out {SERVER_CA_PEM_PATH} -config {SERVER_CA_DETAILS_CONF} -outform PEM"
              
        stdout = subprocess.check_output(cmd, shell=True)        
        
        logger.info('Create server ca symlink')
        stdout = subprocess.check_output(f'openssl x509 -hash -serial -noout -in {SERVER_CA_PEM_PATH}', shell=True)
        hash_name, serial = stdout.decode().splitlines()
        #serial = serial.split('=')[1]
    
        index = 0
        while True:
            candidate = os.path.join(certificates_path_server_client_sym, hash_name + '.' + str(index))
            if not os.path.exists(candidate):
                break
            index += 1
        os.symlink(f'../../server-cert/{SERVER_CA_PEM}' , candidate)    
    
        logger.info('Generate client default certificate CSR')

#        2) Create client default certificate
    client_default_key = CLIENT_KEY_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)
    client_default_pem = CLIENT_PEM_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME)
    #organization = ssl_helper.CLIENT_DEFAULT_CERT_NAME
    #we might want to obfuscate organization
    #organization = str(abs(hash(organization)) % (10 ** 8))
    organization = uuid.uuid4().hex

    if no_ca:    
        update_conf(CSR_TEMPLATE_CONF, CSR_CONF, {'O':organization})                    
        cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -keyout {client_default_key} -out {client_default_pem} -config {CSR_CONF}"
        stdout = subprocess.check_output(cmd, shell=True)        
        shutil.copy(client_default_pem, SERVER_CERTS_PEM_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME))
    else:        
        #create csr and sign it with server_ca
        if not os.path.exists(SERVER_CA_CSR_CONF):
            with open(SERVER_CA_CSR_CONF, 'w') as fd:
                fd.write(CA_CSR_CREATE_CNF.format(org=organization))
        else:
            logger.info(f'Using preexisting {SERVER_CA_CSR_CONF}')
        
        create_csr_cmd = f"openssl req -config {SERVER_CA_CSR_CONF} -newkey rsa:2048 -sha256 -nodes -keyout "\
                        f"{client_default_key} -out {SERVER_CA_CSR_PEM_PATH} -outform PEM"
        stdout = subprocess.check_output(create_csr_cmd, shell=True)              
    
        logger.info('Sign client default certificate CSR')

        # Create the index file
        index_file_path = os.path.join(certificates_path_server_server, 'index.txt')       
        if not os.path.exists(index_file_path):
            open(index_file_path, 'w').close()
                
        create_certificate_cmd = f"openssl ca -rand_serial -batch -policy signing_policy -config {SERVER_CA_DETAILS_CONF} "\
                                f"-extensions signing_req -out {client_default_pem} -infiles {SERVER_CA_CSR_PEM_PATH}"
        stdout = subprocess.check_output(create_certificate_cmd, shell=True)              
        
        shutil.copy(client_default_pem, SERVER_CERTS_PEM_PATH.format(ssl_helper.CLIENT_DEFAULT_CERT_NAME))


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
            
def replace_server_certificate(logger, server_certificate_path=None, certificates_directory_path=None, revert=False):
    '''server_certificate_path should be the path to server.pem, where server.key also exists'''
    if not server_certificate_path and not revert:
        logger.warning('replace_server_certificate bad arguments')
        return False
    
    ssl_helper = SSL_helper(logger, is_server=True, certificates_directory_path=certificates_directory_path, tool_only=True)
    backup_server_pem = ssl_helper.SERVER_PEM_PATH+'.org'
    backup_server_key = ssl_helper.SERVER_KEY_PATH+'.org'
    
    if revert:
        logger.info('Reverting server certificate to original')
        shutil.move(backup_server_pem, ssl_helper.SERVER_PEM_PATH)
        shutil.move(backup_server_key, ssl_helper.SERVER_KEY_PATH)
        return True
    
    logger.info(f'Setting new server certificate from {server_certificate_path}')
    shutil.copy(ssl_helper.SERVER_PEM_PATH, backup_server_pem)
    shutil.copy(ssl_helper.SERVER_KEY_PATH, backup_server_key)
    shutil.copy(server_certificate_path, ssl_helper.SERVER_PEM_PATH)
    shutil.copy(server_certificate_path.replace('.'+ssl_helper.CERT_NAME_EXTENSION, '.'+ssl_helper.KEY_NAME_EXTENSION),
                                                        ssl_helper.SERVER_KEY_PATH)
    #set owner (current user) and permissions (taken from original server pem/key)
    current_uid, current_gid = os.getuid(), os.getgid()
    shutil.copystat(backup_server_pem, ssl_helper.SERVER_PEM_PATH)
    shutil.copystat(backup_server_key, ssl_helper.SERVER_KEY_PATH)
    shutil.chown(ssl_helper.SERVER_PEM_PATH, user=current_uid, group=current_gid)
    shutil.chown(ssl_helper.SERVER_KEY_PATH, user=current_uid, group=current_gid)
    return True


        
        
        
