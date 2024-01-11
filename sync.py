
import boto3
import csv
from datetime import datetime
import os
import pathlib
import argparse
import logging

LOGGER = logging.basicConfig(
    format='%(asctime)s %(message)s',
    filename='sync.log', encoding='utf-8',
    level=logging.INFO
)


parser = argparse.ArgumentParser(
    prog="s3syncer",
    description="uplaods / downloads files from s3"
)
parser.add_argument("-u", "--upload", action="store_true")
parser.add_argument("-d", "--download", action="store_true")
# parser.add_argument("-s", "--sync") # todo

s3 = boto3.client('s3')


def upload(path, bucket, legal_extensions):
    for dirpath, dirnames, files in os.walk(path):
        for file in files:
            if valid_extension(file, legal_extensions):
                    logging.info(f"found file with legal extension: {file}")
                    fpath = (pathlib.Path(dirpath) / file).as_posix()
                    with open(fpath, 'rb') as fileobj:
                        s3.upload_fileobj(fileobj, bucket, file)
    

    with open('./update_log.txt', 'a') as update_log:
        update_log.write( '\n' + 'Synced with S3 on ' + str(datetime.now()))


def download(path, bucket, legal_extensions):
    for item in s3.list_objects(Bucket=bucket)['Contents']:
        file = item["Key"]
        if valid_extension(file, legal_extensions):
            logging.info(f"found item for download {item['Key']} {item['LastModified']}")
            fpath = (pathlib.Path(path) / file).as_posix()
            with open(fpath, 'wb') as f:
                s3.download_fileobj(bucket, item["Key"], f)


def valid_extension(file, legal_extensions):
    for extension in legal_extensions:
        if file[-len(extension):] == extension:
            return True
    return False


def get_sync_paths():
    with open('folder_sync_registrer.txt', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            # first one is local path to ER save dir, second is gdrive folder id
            return row[0], row[1]

def get_extensions_to_sync():
    legal_extensions = []
    with open('extension_whitelist.txt', 'r') as f:
        for line in f.readlines():
               legal_extensions.append(line.strip())
    return legal_extensions


if __name__ == "__main__":
    args = parser.parse_args()
    save_path, bucket = get_sync_paths()
    legal_extensions = get_extensions_to_sync()
    logging.info(f"Starting run {str(args)}")
    if args.upload:
        upload(save_path, bucket, legal_extensions)	
    if args.download:
        download(save_path, bucket, legal_extensions)
    logging.info(f"END RUN\n")

