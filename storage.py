import boto3
from contextlib import contextmanager
import hashlib
import sqlite3
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type


LOCAL_DB = "filesync.db"
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


def update_local(bucket, path, file, modified_time):
    """Store file info after download/upload

    After downloading a file from s3 save the time we got it from s3 (now), the
    last modified time from s3, the filename(its s3 object path), the bucket,
    and the hash of the file. All local data stored in sqlite3

    bucket[str]: name of the bucket in s3
    path[str or posixpath]: path to the local file
    file[str]: the name of file
    modified_time[datetime.datetime]: time aws s3 has as the last modified time for the file.
    """
    pass

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
    return file_data or None

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
    wait=wait_fixed(1),
    retry=retry_if_exception_type(HttpStatusException),
    stop=stop_after_attempt(2)
)
def get_remote_hash(file: str, bucket: str):
    remote = boto3.resource('dynamodb')
    table = remote.Table("filesync")
    response = table.get_item(Key={"bucketfile":f"{bucket}_{file}"})
    status_code = response['ResponseMetadata']['HTTPStatusCode']

    if 200 <= response['ResponseMetadata']['HTTPStatusCode'] < 300:
        item = response['Item']
        return item["hash"], item["last_modified"]

    #otherwise freakout i guess
    raise HttpStatusException(
        "Dynamo DB request returned non 2xx status: {}".format(
            response['ResponseMetadata']
        )
    )


def get_hash(path, buffer_size=65536):
    """Get the sha1 hash of the the file passed"""
    sha1 = hashlib.sha1()
    with open(path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()
