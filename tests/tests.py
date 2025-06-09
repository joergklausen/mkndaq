import os
import unittest

# import paramiko

from mkndaq.utils.sftp import SFTPClient
from mkndaq.utils.utils import load_config

config = load_config('dist/mkndaq.yml')

class TestSFTP(unittest.TestCase):
    def test_config_host(self):
        self.assertEqual(config['sftp']['host'], 'sftp.meteoswiss.ch')

    def test_is_alive(self):
        sftp = SFTPClient(config=config)
        self.assertEqual(sftp.is_alive(), True)


    def test_setup_remote_path(self):
        sftp = SFTPClient(config=config)

        # setup
        file_path = 'test/hello_world.txt'
        remote_path = os.path.join(sftp.remote_path, os.path.dirname(file_path))

        # test
        sftp.setup_remote_path(remote_path)
        self.assertEqual(sftp.remote_item_exists(remote_path=remote_path), True)

        # clean up
        sftp.remove_remote_item(remote_path=remote_path)


    def test_transfer_single_file(self):
        """
        Put a single file from the local file system to the root location of the remote sftp server.
        If the remote destination is a subfolder, this will probably fail.
        """
        sftp = SFTPClient(config=config)

        # setup
        file_path = 'tests/data/hello_world.txt'
        file_path = 'C:/Users/mkn/Documents/mkndaq/staging/hello_world.txt'
        file_content = 'Hello, world!'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as fh:
            fh.write(file_content)
            fh.close()

        remotepath = sftp.remote_path
        remote_path = os.path.join(remotepath, os.path.basename(file_path))
        if sftp.remote_item_exists(remote_path=remote_path):
            sftp.remove_remote_item(remote_path=remote_path)

        # test
        attr = sftp.put_file(local_path=file_path, remote_path=remotepath)

        self.assertEqual(sftp.remote_item_exists(remote_path=remote_path), True)

        # clean up
        sftp.remove_remote_item(remote_path=remote_path)
        os.remove(path=file_path)


    def test_transfer_files(self):
        sftp = SFTPClient(config=config, name="test")

        # setup
        # local_path = '/c/Users/mkn/Documents/mkndaq/staging/fidas'
        local_path = 'tests/data/test'
        # local_path = str()
        remote_path = sftp.remote_path
        # remote_path = str()

        # test
        sftp.transfer_files(local_path=local_path, remote_path=remote_path, remove_on_success=False)

        # clean up
        # for file in os.listdir(local_path):
        #     sftp.remove_remote_item(os.path.join(remote_path, file))
        # sftp.remove_remote_item(remote_path=remote_path)

    def test_transfer_ne300_files(self):
        sftp = SFTPClient(config=config)

        # setup
        local_path = 'tests/data/ne300/'
        remote_path = f"{sftp.remote_path}/ne300"

        # test
        sftp.transfer_files(local_path=local_path, remote_path=remote_path, remove_on_success=False)

        self.assertTrue(len(sftp.transferred) > 0)

if __name__ == '__main__':
    unittest.main()
