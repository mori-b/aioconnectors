import os
import pwd,grp
import time
import logging
import sys
import stat
import subprocess
import re

PYTHON_VERSION = (sys.version_info.major,sys.version_info.minor)
if PYTHON_VERSION < (3,6):
    print('aioconnectors minimum requirement : Python 3.6')
    sys.exit(1)
PYTHON_GREATER_37 = (PYTHON_VERSION >= (3,7))

DEFAULT_LOGGER_NAME = 'aioconnector'
LOGFILE_DEFAULT_PATH = 'aioconnectors.log'
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_SHORT = '%(asctime)s - %(levelname)s - %(message)s'

IPADDR_REGEX = re.compile('inet (?P<ipaddr>[\d\.]+)')

def get_logger(logfile_path=LOGFILE_DEFAULT_PATH, first_run=False, silent=True, logger_name=DEFAULT_LOGGER_NAME, 
               log_format=LOG_FORMAT, level=LOG_LEVEL):
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
            fh.setLevel(logging.DEBUG)        
            logger.addHandler(fh)
    else:
        logger.addHandler(logging.NullHandler())    
    
    return logger


def full_path(the_path):
    if the_path is not None:
        return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(the_path))))

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
        GID_NOGROUP = grp.getgrnam("nogroup").gr_gid
        os.chown(directory_path, UID_NOBODY, GID_NOGROUP, follow_symlinks = False)
        os.chmod(directory_path, stat.S_IRWXU | stat.S_IRWXG)# | stat.S_IRWXO)
    except Exception as exc:
        if logger:
            logger.info('chown_nobody_permissions : '+str(exc))  
    
def iface_to_ip(iface, logger=None):
    try:
        ifconfig_output = subprocess.check_output(['ip', 'addr', 'show', iface], encoding='utf8', timeout=5)
        return IPADDR_REGEX.search(ifconfig_output).group('ipaddr')
    except Exception:
        if logger:
            logger.exception('iface_to_ip')
        return iface

class CustomException(Exception):
    pass