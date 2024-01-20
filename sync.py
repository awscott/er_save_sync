#!/usr/bin/env python

#----------------------+
# poor mans steamcloud |
#----------------------+
import argparse
import boto3
import csv
from datetime import datetime
import logging
import os
import pathlib
import traceback

from storage import (
    get_hash,
    get_file_time,
    get_local_saved_hash,
    get_remote_hash,
    save_local_hash,
    save_remote_hash,
)
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
parser.add_argument("-s", "--sync", action="store_true")
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

def run_sync(target_dir: str, bucket: str, legal_extensions: str):
    # find local files n sync em
    # check remote for files matching legal extensions,
    #   if not in local_files download each
    local_files = set([
        file for file in os.listdir(target_dir)
        if os.path.isfile(pathlib.Path(target_dir) / file)
        and valid_extension(file, legal_extensions)
    ])
    remote_files = set([
        item["Key"] for item in s3.list_objects(Bucket=bucket)['Contents']
        if valid_extension(item["Key"], legal_extensions)
    ])
    new_remote = remote_files - local_files # need to download these

    synced = []
    for file in local_files:
        synced.append(sync(
            path=(pathlib.Path(target_dir) / file).as_posix(),
            bucket=bucket,
        ))

    # download new files
    for file in new_remote:
        remote_hash, remote_last_modified = get_remote_hash(file, bucket)
        synced.append(download_file(
            file,
            (pathlib.Path(target_dir) / file).as_posix(),
            bucket,
            remote_hash,
            remote_last_modified,
        ))

    # decide what to say
    all_synced = local_files | new_remote
    if all(res[0] for res in synced):
        logging.info("Fully successful sync!")
        msg("ALL FILES SYNCED! :)\n"+'\n'.join(all_synced))
    else:
        logging.warning("PARTIAL OR TOTAL FAILURE OF SYNC")
        msg_text = "hmmm... :/\n"
        for result, messsage in synced:
            msg_text += messsage + '\n'
            if result:
                logging.info(messsage)
            else:
                logging.warning(messsage)
        msg(msg_text)


def sync(path: str, bucket: str) -> (bool, str):
    """sync file with s3.
    path: path to file on disk to sync
    bucket: the bucket that hosts that file on remote

    Returns: bool and str representing if successful or not and what the result
        of the sync was
    """
    filename = os.path.basename(path)
    file_last_modified = get_file_time(path)

    file_hash = get_hash(path)
    local_hash, local_last_modified  = get_local_saved_hash(filename, bucket)
    remote_hash, remote_last_modified = get_remote_hash(filename, bucket)

    local_updated = False
    if file_hash != local_hash or local_hash is None:
        local_updated = True

    if file_hash == remote_hash and local_hash is None:
        recent_filetime = max([file_last_modified, local_last_modified, remote_last_modified])
        save_local_hash(filename, bucket, file_hash, recent_filetime)
        save_remote_hash(filename, bucket, file_hash, recent_filetime)
        return True, f"{filename} is already up to date, updated times to set this to most recent version"

    if local_hash == remote_hash and local_updated:
        return upload_file(filename, path, bucket, file_hash, file_last_modified)
    elif local_hash != remote_hash and local_updated:
        warning = (
            "Files Out Of Sync Warning: a play sesh or file modification has "
            "happened locally while files not in sync with remote. i.e. you "
            "likey played on two devicees with out syncing inbetween. "
            "Manual intervention is needed.\n\t"
            f"{filename} modified at {file_last_modified}, was last synced for"
            f" a version at {local_last_modified} while REMOTE version is at "
            f"{remote_last_modified}."
            "\n\tfile hash:{}\n\tlast synced hash: {}\n\tremote_hash:{}".format(
                file_hash, local_hash, remote_hash
            )
        )
        logging.warning(warning)
        save_local_hash(filename, bucket, file_hash, file_last_modified)
        return False, warning
    elif local_hash != remote_hash and not local_updated:
        # download time (i hope)
        if remote_last_modified is None or local_last_modified <= remote_last_modified:
            return download_file(
                filename,
                path,
                bucket,
                file_hash,
                remote_last_modified,
            )
        else:
            warning = (
                "since last download/upload we havent changed the file"
                "and our hash is differnt however our file still looks to"
                "be newer. this means the db state is messed up and needs"
                "attention. run and upload or download manual task."
            )
            logging.warning(warning)
            return False, warning
    else:
        return True, f"{filename} is already up to date!"


def upload_file(filename: str, path: str, bucket: str, file_hash: str, file_last_modified: str):
    with open(path, 'rb') as fileobj:
        s3.upload_fileobj(fileobj, bucket, filename)
    try:
        save_local_hash(filename, bucket, file_hash, file_last_modified)
        save_remote_hash(filename, bucket, file_hash, file_last_modified)
        message = f"Successful upload of {filename}..."
        status = True
        logging.info(message)
    except Exception as e:
        tb = traceback.format_exc()
        message = (
            "File Uplaoded to s3 successfully, but storage of new file in "
            "local/remote DB FAILED."
            f"\n\tfilename:{filename}\n\tbucket:{bucket}\n\tfile_hash:{file_hash}\n"
            f"\tfile_last_modified:{file_last_modified}\n"
            f"{str(e)} {tb}"
        )
        status = False
        logging.error(message)
    return status, message


def download_file(filename: str, path: str, bucket: str, file_hash: str, remote_last_modified: str):
    with open(path, 'wb') as fileobj:
        s3.download_fileobj(bucket, filename, fileobj)
    try:
        if remote_last_modified is None:
            new_file_hash = get_hash(path)
            save_local_hash(filename, bucket, file_hash, new_file_hash)
            save_remote_hash(filename, bucket, file_hash, new_file_hash)
        else:
            save_local_hash(filename, bucket, file_hash, remote_last_modified)
        message = f"Successful download of {filename}..."
        status = True
        logging.info(message)
    except Exception as e:
        tb = traceback.format_exc()
        message = (
            "File Downloaded from s3 successfully, but storage of new file in "
            "local DB FAILED."
            f"\n\tfilename:{filename}\n\tbucket:{bucket}\n\tfile_hash:{file_hash}\n"
            f"\tremote_last_modified:{remote_last_modified}\n"
            f"{str(e)} {tb}"
        )
        status = False
        logging.error(message)
    return status, message


@catch_exc
def upload(path, bucket, legal_extensions):
    """walk dir tree and upload all requested shiz"""
    sync_type = "Upload"
    uploaded = []
    for dirpath, dirnames, files in os.walk(path):
        for file in files:
            if valid_extension(file, legal_extensions):
                    logging.info(f"found file with legal extension: {file}")
                    fpath = (pathlib.Path(dirpath) / file).as_posix()
                    with open(fpath, 'rb') as fileobj:
                        s3.upload_fileobj(fileobj, bucket, file)
                    uploaded.append(file)
                    try:
                        file_hash = get_hash(fpath)
                        file_last_modified = get_file_time(fpath)
                        save_local_hash(file, bucket, file_hash, file_last_modified)
                        save_remote_hash(file, bucket, file_hash, file_last_modified)
                    except Exception as e:
                        logging.warning("File Upload Success but syncing local and remote hashes failed")
                        logging.warning(e)
                        sync_type = "Uploaded but saving hashes fail"

    log_sync(uploaded, sync_type)
    return uploaded


@catch_exc
def download(path, bucket, legal_extensions):
    """download everything in bucket to target dir"""
    sync_type = "Download"
    downloaded = []
    for item in s3.list_objects(Bucket=bucket)['Contents']:
        file = item["Key"]
        if valid_extension(file, legal_extensions):
            downloaded.append(get_object(path, bucket, item, file))
            try:
                fpath = (pathlib.Path(path) / file).as_posix()
                file_hash = get_hash(fpath)
                file_last_modified = get_file_time(fpath)
                save_local_hash(file, bucket, file_hash, file_last_modified)
                save_remote_hash(file, bucket, file_hash, file_last_modified)
            except Exception as e:
                logging.warning("File Download Success but syncing local and remote hashes failed")
                logging.warning(e)
                sync_type = "Downloaded but saving hashes fail"

    log_sync(downloaded, sync_type)
    return downloaded

def get_object(path, bucket, item, file):
    logging.info(f"found item for download {item['Key']} {item['LastModified']}")
    fpath = (pathlib.Path(path) / file).as_posix()
    with open(fpath, 'wb') as f:
        s3.download_fileobj(bucket, file, f)
    return file


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
    if args.sync:
        run_sync(
            target_dir=save_path,
            bucket=bucket,
            legal_extensions=legal_extensions,
        )
    elif args.upload:
        upload(save_path, bucket, legal_extensions)
    elif args.download:
        download(save_path, bucket, legal_extensions)
    else:
        logging.warning("Unrecognized arguments given")
        msg(f"unrecognized args given; {argparse.format_usage()}")
    logging.info(f"END RUN\n")
