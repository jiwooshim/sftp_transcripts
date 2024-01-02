import paramiko
import os
import logging
import traceback as tb
from datetime import datetime
import sys
import traceback as tb
from stat import S_ISDIR
from typing import Literal
from dotenv import load_dotenv

today = datetime.now().strftime("%Y%m%d")

load_dotenv()

# Specify source SFTP details
SOURCE_HOST = os.environ.get('SOURCE_HOST')
SOURCE_PORT = int(os.environ.get('SOURCE_PORT'))
SOURCE_USERNAME = os.environ.get('SOURCE_USERNAME')
SOURCE_PASSWORD = os.environ.get('SOURCE_PASSWORD')

# Specify destination SFTP details
DESTINATION_HOST = os.environ.get('DESTINATION_HOST')
DESTINATION_PORT = int(os.environ.get('DESTINATION_PORT'))
DESTINATION_USERNAME = os.environ.get('DESTINATION_USERNAME')
DESTINATION_PASSWORD = os.environ.get('DESTINATION_PASSWORD')

# Set directories
SOURCE_DIR = os.environ.get("SOURCE_DIR")
DESTINATION_DIR = os.environ.get('DESTINATION_DIR')
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "files")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE_NAME = f'ftp_transcripts_{today}.log'
LOG_FILE_PATH = os.path.join(LOGS_DIR, LOG_FILE_NAME)

if not os.path.exists(LOCAL_DIR):
    os.mkdir(LOCAL_DIR)
if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)



def get_logger():
    """Set up logging"""
    logger = logging.getLogger('sftp_transcripts_bmz988')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s - %(message)s', datefmt='%Y%m%d %H:%M:%S')

    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    file_handler.set_name('my_file_handler')
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    console_handler.set_name('my_console_handler')
    logger.addHandler(console_handler)

    logger.info('=' * 65)
    logger.info(
        f"Process started at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
    logger.info('Command line:\t{0}'.format(sys.argv))
    logger.info('=' * 65)
    return logger


# Set up logging
logger = get_logger()


def connect_sftp(host, port, username, password):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def close_sftp(sftp, transport):
    sftp.close()
    transport.close()


def init_sftp(host, port, username, password):
    try:
        logger.info(
            f"Connecting to the SFTP Server at "
            f"host: {host} "
            f"port: {port} "
            f"username: {username}")
        source_sftp, source_transport = connect_sftp(host, port, username, password)
        return source_sftp, source_transport
    except:
        logger.error(tb.print_exc())
        logger.error(f"Failed to connect to the source\n")
        return 
    

def get_file_list(sftp_client: None, directory: str) -> list[str]:
    try:
        def get_list(_client, _dir):
            file_list = []
            child_file_list = []
            _file_list_attr = sftp_client.listdir_attr(_dir)
            for _file_attr in _file_list_attr:
                if S_ISDIR(_file_attr.st_mode):
                    child_file_list += get_list(_client, os.path.join(_dir, _file_attr.filename))
                else:
                    file_list.append(os.path.join(_dir, _file_attr.filename))
            return file_list + child_file_list
        file_list_recursive = get_list(sftp_client, directory)
        # Remove top directory
        return file_list_recursive, [file.replace(directory, '') for file in file_list_recursive]
        
    except:
        logger.error(tb.print_exc())
        logger.error(f"Failed to retrieve file list\n")
        return 


def mkdir_recursive(file_dir: str, sftp_client=None, location: str = Literal['local', 'sftp']) -> None:
    if not os.path.isdir(file_dir):
        _dir = os.path.dirname(file_dir) 
    else: 
        _dir = os.path.abspath(file_dir)
    if location == 'local':
        if not os.path.exists(_dir):
            os.makedirs(_dir)
    elif location == 'sftp':
        try: 
            sftp_client.listdir(_dir)
        except:
            try: 
                sftp_client.mkdir(_dir)
            except:
                mkdir_recursive(os.path.dirname(_dir), sftp_client, location='sftp')
    return 
    

def main():
    # Connect to the source and destination SFTP servers
    source_sftp, source_transport = init_sftp(SOURCE_HOST, SOURCE_PORT, SOURCE_USERNAME, SOURCE_PASSWORD)
    destination_sftp, destination_transport = init_sftp(DESTINATION_HOST, DESTINATION_PORT, 
                                                        DESTINATION_USERNAME, DESTINATION_PASSWORD)

    # Get the list of files in the source directory
    logger.info(f"Retrieving the file list from the source directory")
    source_file_list, source_file_list_normalized = get_file_list(source_sftp, SOURCE_DIR)

    # Get the list of files in the destination directory
    logger.info(f"Retrieving the file list from the destination directory")
    destination_file_list, destination_file_list_normalized = get_file_list(destination_sftp, DESTINATION_DIR)

    # Copy each file from the source directory to the destination directory
    downloaded_local_files = []
    copied_files_count = 0
    for file_name in source_file_list_normalized:
        source_file_path = os.path.join(SOURCE_DIR, *file_name.split("/"))
        local_file_path = os.path.join(LOCAL_DIR, *file_name.split("/"))
        destination_file_path = os.path.join(DESTINATION_DIR, *file_name.split("/"))
    
        if destination_file_path in destination_file_list:
            continue
        try:
            logger.info(f"Source: {source_file_path}")
            logger.info(f"Local: {local_file_path}")
            logger.info(f"Destination: {destination_file_path}")
            mkdir_recursive(local_file_path, location='local')
            source_sftp.get(source_file_path, local_file_path)
            logger.info(f"Successfully copied source to local")
            downloaded_local_files.append(local_file_path)
            mkdir_recursive(destination_file_path, destination_sftp, location='sftp')
            destination_sftp.put(local_file_path, destination_file_path)
            logger.info(f"Successfully copied local to destination")
            copied_files_count += 1
        except:
            logger.error(tb.print_exc())
            logger.error(f"Failed to copy file: source: {source_file_path}")
            continue
        if copied_files_count > 10:
            break 

    logger.info(f"Successfully copied {copied_files_count} files to destination files")
    
    # Close the SFTP connections
    logger.info(f"Closing SFTP connection")
    close_sftp(source_sftp, source_transport)
    close_sftp(destination_sftp, destination_transport)

    # Remove locally downloaded files
    logger.info(f"Removing files from local directory")
    logger.info(f"List of files to remove: \n{downloaded_local_files}")

    for local_file_path in downloaded_local_files:
        logger.info(f"Removing file: {local_file_path}")
        # Please check the file directory to avoid removing other file
        os.remove(local_file_path)

    logger.info(
        f"Process ended at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")


if __name__ == "__main__":
    try:
        main()
    except:
        logger.error(tb.print_exc())
    try:
        # Send log file to the destination server
        destination_sftp, destination_transport = init_sftp(DESTINATION_HOST, DESTINATION_PORT,
                                                            DESTINATION_USERNAME, DESTINATION_PASSWORD) 
        destination_file_path = os.path.join(DESTINATION_DIR, 'logs', LOG_FILE_NAME)
        mkdir_recursive(destination_file_path, destination_sftp, location='sftp')
        destination_sftp.put(LOG_FILE_PATH, destination_file_path)
        close_sftp(destination_sftp, destination_transport)
    except:
        logger.error(tb.print_exc())
        