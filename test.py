from datetime import datetime
import os
import tempfile
import unittest
import unittest.mock as mock
import sync

MOCK_ZENITY = mock.Mock()  
sync.msg = MOCK_ZENITY

class TestSync(unittest.TestCase):
    def setUp(self) -> None:     
        return super().setUp()

    @mock.patch("sync.log_sync")
    @mock.patch("sync.s3")
    @mock.patch("sync.boto3")
    def test_dowload_returns_files_downloaded(self, mock_boto, mock_s3, mock_log_sync):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(b"This is a testfile!")
            fp.seek(0)
            file_name, file_dir = os.path.basename(fp.name), os.path.abspath(os.path.dirname(fp.name))
            # make it return the file we want
            mock_s3.list_objects.return_value = {
                'Contents':
                    [
                        {
                            "Key": file_name,
                            "LastModified": datetime(2020, 1, 1, 0, 0, 0)
                        }
                    ]
            }
            mock_s3.list_objects.assert_called_once
            mock_s3.download_fileobj.assert_called_once
            files = sync.download(file_dir, "testbucket", None)
            assert len(files) == 1
            assert files[0] == file_name

    @mock.patch("sync.log_sync")
    @mock.patch("sync.s3")
    @mock.patch("sync.boto3")
    def test_dowload_checks_hash(self, mock_boto, mock_s3, mock_log_sync):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(b"This is a testfile!")
            fp.seek(0)
            file_name, file_dir = os.path.basename(fp.name), os.path.abspath(os.path.dirname(fp.name))
            # make it return the file we want
            mock_s3.list_objects.return_value = {
                'Contents':
                    [
                        {
                            "Key": file_name,
                            "LastModified": datetime(2020, 1, 1, 0, 0, 0)
                        }
                    ]
            }
            mock_s3.list_objects.assert_called_once
            mock_s3.download_fileobj.assert_called_once
            files = sync.download(file_dir, "testbucket", None)
            assert len(files) == 1
            assert files[0] == file_name


if __name__ == '__main__':
    unittest.main()