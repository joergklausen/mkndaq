import os
from pathlib import Path, PurePosixPath
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
        sftp = SFTPClient(config=config, name='test')

        # setup
        remote_path = PurePosixPath(sftp.remote_path) / sftp.name

        # test
        sftp.setup_remote_path(remote_path)
        self.assertEqual(sftp.remote_item_exists(remote_path=remote_path), True)

        # clean up
        sftp.remove_remote_item(remote_path=remote_path)


    def test_transfer_single_file(self):
        """
        Put a single file from the local file system to remote sftp server.
        If name exists, local files will be transfered to a remote folder name.
        After transfer, clean up.
        """
        sftp = SFTPClient(config=config, name='test')

        # setup
        local_path = Path('tests/data/test/hello/hello.txt')

        # test
        sftp.transfer_files(local_path=local_path, remove_on_success=False)

        self.assertEqual(len(sftp.transfered_remote) > 0, True)

        # clean up
        for remote_path in sftp.transfered_remote:
            sftp.remove_remote_item(remote_path=remote_path, recursive=True)


    def test_transfer_files(self):
        sftp = SFTPClient(config=config, name="test")

        # setup
        # local_path = '/c/Users/mkn/Documents/mkndaq/staging/fidas'
        local_path = Path('tests/data/test')

        # test
        sftp.transfer_files(local_path=local_path, remove_on_success=False)

        self.assertTrue(len(sftp.transfered_remote) > 0)

        # clean up
        for remote_path in sftp.transfered_remote:
            sftp.remove_remote_item(remote_path=remote_path, recursive=True)


    def test_transfer_ne300_files(self):
        sftp = SFTPClient(config=config, name="ne300")

        # setup
        local_path = Path('tests/data/ne300/')
        # remote_path = PurePosixPath(sftp.remote_path)

        # test
        # sftp.transfer_files(local_path=local_path, remote_path=remote_path, remove_on_success=False)
        sftp.transfer_files(local_path=local_path, remove_on_success=False)

        self.assertTrue(len(sftp.transfered_remote)==9)

if __name__ == '__main__':
    unittest.main()
