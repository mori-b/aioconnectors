import os
import pwd,grp
import time
import logging
from logging.handlers import RotatingFileHandler
import sys
import stat
import subprocess
import re
import gzip


PYTHON_VERSION = (sys.version_info.major,sys.version_info.minor)
if PYTHON_VERSION < (3,6):
    print('aioconnectors minimum requirement : Python 3.6')
    sys.exit(1)
PYTHON_GREATER_37 = (PYTHON_VERSION >= (3,7))

DEFAULT_LOGGER_NAME = 'aioconnector'
LOGFILE_DEFAULT_PATH = 'aioconnectors.log'
LOG_LEVEL = 'INFO'
LOG_ROTATE = True
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_SHORT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_BK_COUNT = 5
LOG_MAX_SIZE = 67108864 # 2**26 = 64 MB

SOURCE_ID_REGEX = re.compile('^[0-9a-zA-Z-_:]+$')
SOURCE_ID_DEFAULT_REGEX = re.compile('^default[0-9]*$')
SOURCE_ID_MAX_LENGTH = 128

def full_path(the_path):
    if the_path is not None:
        return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))

def get_tmp_dir():
    if os.path.exists('/var/tmp'):
        return '/var/tmp/aioconnectors'
    else:
        candidate1 = full_path('~/aioconnectors_tmp')
        candidate2 = full_path('aioconnectors_tmp')
        return min(candidate1, candidate2, key=lambda x:len(x))

def get_logger(logfile_path=LOGFILE_DEFAULT_PATH, first_run=False, silent=True, logger_name=DEFAULT_LOGGER_NAME, 
               log_format=LOG_FORMAT, level=LOG_LEVEL, rotate=LOG_ROTATE):
    
    def namer(name):
        return name + '.gz'
    
    def rotator(source, dest):
        with open(source, 'rb') as sf:
            data = sf.read()
        compressed = gzip.compress(data)
        with open(dest, 'wb') as df:
            df.write(compressed)
        os.truncate(source, 0)
    
    logger = logging.getLogger(logger_name)
    logger.handlers = []
    if not first_run:
        handlers = []
        if logfile_path:    #could be '' if no config file provided
            if rotate:
                if rotate is True:
                    rotate = LOG_MAX_SIZE
                else:
                    #use user defined value                    
                    try:
                        rotate = int(rotate)
                    except Exception:
                        rotate = LOG_MAX_SIZE
                fh = RotatingFileHandler(logfile_path, maxBytes=rotate, backupCount=LOG_BK_COUNT)
                fh.rotator = rotator
                fh.namer = namer                
                handlers.append(fh)
            else:
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
            fh.setLevel(logging.DEBUG)        
            logger.addHandler(fh)
    else:
        logger.addHandler(logging.NullHandler())    
    
    return logger

def chown_file(filepath, username, groupname, logger=None):
    try:
        uid = pwd.getpwnam(username).pw_uid
        gid = grp.getgrnam(groupname).gr_gid
        os.chown(filepath, uid, gid, follow_symlinks = False)
    except Exception:
        if logger:
            logger.exception('chown_file')        

def chown_nobody_permissions(directory_path, logger=None):
    try:
        UID_NOBODY = pwd.getpwnam("nobody").pw_uid
        try:
            GID_NOGROUP = grp.getgrnam("nogroup").gr_gid
        except Exception:
            GID_NOGROUP = grp.getgrnam("nobody").gr_gid
        os.chown(directory_path, UID_NOBODY, GID_NOGROUP, follow_symlinks = False)
        os.chmod(directory_path, stat.S_IRWXU | stat.S_IRWXG)# | stat.S_IRWXO)
    except Exception as exc:
        if logger:
            logger.info('chown_nobody_permissions : '+str(exc))  
    
def iface_to_ip(iface, logger=None):
    try:
        ifconfig_output = subprocess.check_output(['ip', 'addr', 'show', iface], encoding='utf8', timeout=5)
        return re.search(f'inet (?P<ipaddr>[\d\.]+).*{iface}$', ifconfig_output, re.MULTILINE).group('ipaddr')
    except Exception:
        if logger:
            logger.exception('iface_to_ip')
        return iface

def validate_source_id(source_id):
    if not SOURCE_ID_REGEX.match(source_id):
    #if '.' in source_id or '/' in source_id:
        #protect against path traversal
        raise Exception(f'Invalid source_id : {source_id} - please use only {SOURCE_ID_REGEX.pattern}')
    if len(source_id) > SOURCE_ID_MAX_LENGTH:
        raise Exception(f'Invalid source_id : {source_id} - of length {len(source_id)}')        
    if SOURCE_ID_DEFAULT_REGEX.match(source_id):
        raise Exception(f'Invalid source_id : {source_id} - cannot match {SOURCE_ID_DEFAULT_REGEX.pattern}')
        
class CustomException(Exception):
    pass
