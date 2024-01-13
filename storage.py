import hashlib
import sqlite3


def update_local(bucket, path, file, modified_time):
    """Store file info after download

    After downloading a file from s3 save the time we got it from s3 (now), the
    last modified time from s3, the filename(its s3 object path), the bucket,
    and the hash of the file. All local data stored in sqlite3

    bucket[str]: name of the bucket in s3
    path[str or posixpath]: path to the local file
    file[str]: the name of file
    modified_time[datetime.datetime]: time aws s3 has as the last modified time for the file.
    """
    pass


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
