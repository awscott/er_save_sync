
import argparse
import boto3
import csv
from datetime import datetime
import logging
import os
import pathlib
import traceback

from storage import get_hash, get_local_saved_hash

# log to file to check for errors later
def setup_logging(logfile='file_sync.log'):
    LOGGER = logging.basicConfig(
        format='%(asctime)s %(message)s',
        filename=logfile, encoding='utf-8',
        level=logging.INFO
    )

# add args to this script for easy calling in steamdeck game mode
parser = argparse.ArgumentParser(
    prog="s3syncer",
    description="uplaods / downloads files from s3"
)
parser.add_argument("-u", "--upload", action="store_true")
parser.add_argument("-d", "--download", action="store_true")
# parser.add_argument("-s", "--sync") # todo

s3 = boto3.client('s3')

UPDATE_LOG = './update_log.txt'

def msg(message):
    os.system(f"zenity --no-markup --info --text='{message}'")

def catch_exc(func):
    """Handle success / failure of python script with gui shiz"""
    def wrapper(*args, **kwargs):
        try:
            results = func(*args, **kwargs)
        except Exception as e:
            error = f'{func.__name__} Failed.\n {func.__name__} hit this exception: {e.__class__.__name__} {e}. {traceback.format_exc()}'
            logging.error(error)
            msg(error)
        else:
            body = f'Successful {func.__name__}! Synced the following {len(results)} file(s):\n' + '\n'.join(results)
            msg(body)
            return results
    return wrapper

def sync(path, bucket, legal_extensions):
    # check if file has been updated compared to local hash
    #   local_updated = True
    # check if local has same hash as remote
    #   if local == remote && local_updated: upload()
    #   elif local != remote && local_updated: warn_about_out_of_sync()
    #   elif local != remote and not local_updated:
    #       #this means since last download/upload we havent changed the file there must be a new one in s3
    #       if local_last_modified <= remote_last_modified:
    #           download()
    #        else: # this only happens if we fail during upload
    #            msg("since last download/upload we havent changed the file"
    #                "and our hash is differnt however our file still looks to"
    #                "be newer. this means the db state is messed up and needs"
    #                "attention. run and upload or download with --force."
    #            )
    #   elif local == remote && !local_updated:
    #      msg("nothing to do, its already synced!")
    filename = os.path.basename(path)
    file_hash = get_hash(path)
    local_hash = get_local_saved_hash(filename, bucket)

@catch_exc
def upload(path, bucket, legal_extensions):
    uploaded = []
    for dirpath, dirnames, files in os.walk(path):
        for file in files:
            if valid_extension(file, legal_extensions):
                    logging.info(f"found file with legal extension: {file}")
                    fpath = (pathlib.Path(dirpath) / file).as_posix()
                    with open(fpath, 'rb') as fileobj:
                        s3.upload_fileobj(fileobj, bucket, file)
                    uploaded.append(file)

    log_sync(uploaded, "Upload")
    return uploaded


@catch_exc
def download(path, bucket, legal_extensions):
    downloaded = []
    for item in s3.list_objects(Bucket=bucket)['Contents']:
        file = item["Key"]
        if valid_extension(file, legal_extensions):
            logging.info(f"found item for download {item['Key']} {item['LastModified']}")
            fpath = (pathlib.Path(path) / file).as_posix()
            with open(fpath, 'wb') as f:
                s3.download_fileobj(bucket, file, f)
            downloaded.append(file)

    log_sync(downloaded, "Download")
    return downloaded


def log_sync(files, sync_type):
    with open(UPDATE_LOG, 'a') as update_log:
        update_log.write('\n' + f'{sync_type}ed {len(files)} files with S3 on ' + str(datetime.now()))


def valid_extension(file, legal_extensions):
    """Check if file is in allowlist

    file[str]: filename or path
    legal_extensions[list or None]:
        upload files whos extensions are in the list,
        if None upload anything
    """

    if legal_extensions is None:
        return True

    for extension in legal_extensions:
        if file[-len(extension):] == extension:
            return True
    return False


def get_sync_paths():
    with open('folder_sync_config.txt', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            # first one is local path to ER save dir, second is s3 bucket name
            return row[0], row[1]

def get_extensions_to_sync():
    legal_extensions = []
    with open('extension_whitelist.txt', 'r') as f:
        for line in f.readlines():
               legal_extensions.append(line.strip())
    return legal_extensions



if __name__ == "__main__":
    setup_logging()
    args = parser.parse_args()
    save_path, bucket = get_sync_paths()
    legal_extensions = get_extensions_to_sync()
    logging.info(f"Starting run {str(args)}")
    if args.upload:
        upload(save_path, bucket, legal_extensions)
    elif args.download:
        download(save_path, bucket, legal_extensions)
    else:
        sync(save_path, bucket, legal_extensions)
    logging.info(f"END RUN\n")
