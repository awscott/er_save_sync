from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from contextlib import contextmanager
import hashlib
import os
import sqlite3
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type


REMOTE_TABLE = "filesync" # dynamodb (it's cheap okay)
class BucketFile:
    # This defines how we expect our blobs in dynamodb table (filesync) to look
    def __init__(self, filename, bucket, hash, last_modified):
        self.bucketfile = f"{bucket}_{filename}"  # primary key in dynamo
        self.filename = filename
        self.bucket = bucket
        self.hash = hash
        self.lastmodified = last_modified
    
    def json(self):
        return {
            attr: {'S': getattr(self, attr)}
            for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__")
        }

LOCAL_DB = "filesync.db" #sqlite

class HttpStatusException(Exception):
    pass

@contextmanager
def local_cursor():
    connection = sqlite3.connect(LOCAL_DB)
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS files(filename, bucket, hash, last_modified)"
    )
    yield cursor
    cursor.close()
    connection.commit()
    connection.close()


def get_local_saved_hash(file: str, bucket: str):
    with local_cursor() as cur:
        results = cur.execute(
            f"""
            SELECT hash, last_modified FROM files
            WHERE filename='{file}' and bucket='{bucket}'
            """
        )
        file_data = results.fetchone()
        print(file_data)
        if not file_data or file_data == ('None', 'None'):
            return (None, None)
    return file_data


def save_local_hash(file: str, bucket: str, hash: str, last_modified: str):
    with local_cursor() as cur:
        res = cur.execute(
            f"""
            SELECT 1 FROM files WHERE filename='{file}' and bucket='{bucket}'
            """
        )
        data = res.fetchone()
        if data is None:
            sql = f"""
            INSERT INTO files VALUES
            ('{file}','{bucket}','{hash}','{last_modified}')
            """
        else:
            sql = f"""
            UPDATE files SET
              hash='{hash}',
              last_modified='{last_modified}'
            WHERE
              filename='{file}'
              and bucket='{bucket}'
            """
        cur.execute(sql)

@retry(
    wait=wait_fixed(.5),
    retry=retry_if_exception_type(ClientError),
    stop=stop_after_attempt(2)
)
def save_remote_hash(file: str, bucket: str, hash: str, last_modified: str):
    client = boto3.client('dynamodb')
    dynamo_item = BucketFile(file, bucket, hash, last_modified)
    return client.put_item(
        TableName=REMOTE_TABLE,
        Item=dynamo_item.json()
    )


@retry(
    wait=wait_fixed(1),
    retry=retry_if_exception_type(HttpStatusException),
    stop=stop_after_attempt(2)
)
def get_remote_hash(file: str, bucket: str):
    remote = boto3.resource('dynamodb')
    table = remote.Table("filesync")
    response = table.get_item(Key={"bucketfile":f"{bucket}_{file}"})
    status_code = response['ResponseMetadata']['HTTPStatusCode']

    if 200 <= status_code < 300:
        item = response.get('Item')
        if item is None:
            return None, None # doesnt exist in remote
        return item["hash"], item["lastmodified"]

    #otherwise freakout i guess
    raise HttpStatusException(
        "Dynamo DB request returned non 2xx status: {}".format(
            response['ResponseMetadata']
        )
    )


def get_hash(path: str, buffer_size=65536):
    """Get the sha1 hash of the the file passed"""
    sha1 = hashlib.sha1()
    with open(path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()

def get_file_time(path: str):
    file_last_modified = datetime.fromtimestamp(
        os.path.getmtime(path)
    ).astimezone().isoformat()

    return file_last_modified
