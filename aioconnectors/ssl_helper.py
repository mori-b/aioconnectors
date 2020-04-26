import os
import asyncio
import json
import subprocess
import uuid

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


class SSL_helper:
    DEFAULT_BASE_PATH = '/tmp/aioconnectors' #os.getcwd()
    CLIENT_DEFAULT_CERT_NAME = 'default'    
    SOURCE_ID_2_CERT = 'source_id_2_cert.json'
    CERT_NAME_EXTENSION = "pem"
    KEY_NAME_EXTENSION = "key"
    
    def __init__(self, logger, is_server, certificates_directory_path=None):
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
                stdout = subprocess.check_output('openssl x509 -hash -serial -noout -in '+str(os.path.join(self.SERVER_CERTS_PATH, self.CLIENT_DEFAULT_CERT_NAME+'.'+self.CERT_NAME_EXTENSION)), shell=True)
                hash_name, serial = stdout.decode().splitlines()
                self.default_client_serial = serial.split('=')[1]

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
            if common_name:
                create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -subj '/O={common_name}/CN={common_name}' -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
            else:
                #necessary to set a unique field (like organization), so that each certificate has a unique hash, which is better for fast authentication
                organization = uuid.uuid4().hex
                create_certificate_cmd = f"openssl req -new -newkey rsa -nodes -x509 -days 3650 -subj '/O={organization}' -keyout "\
                                         f"{key_path} -out {crt_path} -config {self.CSR_CONF}"
                                     
            proc, stdout, stderr = await self.run_cmd(create_certificate_cmd)
        
            if proc.returncode != 0:
                raise Exception('Error while Generating self signed certificate : '+stderr.decode())
            #if stderr:
            #    self.logger.warning('create_certificate_cmd : '+stderr.decode())
        
            #create symlink for the client certificate named by their fingerprints so they will be detected by context.load_verify_locations(capath=
            #first, calculate hash for symlink name
            proc, stdout, stderr = await self.run_cmd(f'openssl x509 -hash -serial -noout -in {crt_path}')
            if stderr:
                self.logger.warning('hash : '+stderr.decode())            
            hash_name, serial = stdout.splitlines()
            serial = serial.split('=')[1]
            
            #create a symlink called <hash>.<first free index, starting from 0>, pointing to f'../{source_id}.{self.CERT_NAME_EXTENSION}'
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
                            os.rename(os.path.join(self.SERVER_SYMLINKS_PATH, the_file_name+'.'+str(test_index)), os.path.join(self.SERVER_SYMLINKS_PATH, the_file_name+'.'+str(test_index-1)))                                    
                        
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
            