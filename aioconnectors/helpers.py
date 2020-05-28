import os
import pwd,grp
import time
import logging
import sys
import stat

DEFAULT_LOGGER_NAME = 'aioconnector'
LOGFILE_DEFAULT_PATH = 'aioconnectors.log'
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_SHORT = '%(asctime)s - %(levelname)s - %(message)s'


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

def chown_file(filepath, username, groupname):
    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam(groupname).gr_gid
    os.chown(filepath, uid, gid, follow_symlinks = False)  

def chown_nobody_permissions(directory_path):
    UID_NOBODY = pwd.getpwnam("nobody").pw_uid
    GID_NOGROUP = grp.getgrnam("nogroup").gr_gid
    os.chown(directory_path, UID_NOBODY, GID_NOGROUP, follow_symlinks = False)
    os.chmod(directory_path, stat.S_IRWXU | stat.S_IRWXG)# | stat.S_IRWXO)
    