import os
# import polars as pl
import unittest
from mkndaq.utils.utils import load_config
from mkndaq.utils.sftp import SFTPClient

config = load_config('dist/mkndaq.yml')

class TestSFTP(unittest.TestCase):
    def test_config_host(self):
        self.assertEqual(config['sftp']['host'], 'sftp.meteoswiss.ch')

    def test_is_alive(self):
        sftp = SFTPClient(config=config)

        self.assertEqual(sftp.is_alive(), True)

    def test_transfer_single_file(self):
        sftp = SFTPClient(config=config)

        # setup
        file_path = 'tests/data/hello_world.txt'
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

    def test_setup_remote_folders(self):
        sftp = SFTPClient(config=config)

        # setup
        file_path = 'tests/data/hello_world.txt'
        file_content = 'Hello, world!'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as fh:
            fh.write(file_content)
            fh.close()
        remote_path = os.path.join(sftp.remote_path, os.path.dirname(file_path))

        # test
        sftp.setup_remote_folders(local_path=os.path.dirname(os.path.abspath(file_path)), remote_path=remote_path)

        remote_file=os.path.join(remote_path, os.path.basename(file_path))
        attr = sftp.put_file(local_path=file_path, remote_path=remote_file)

        self.assertEqual(sftp.remote_item_exists(remote_path=remote_file), True)

        # clean up
        sftp.remove_remote_item(remote_path=remote_file)            
        sftp.remove_remote_item(remote_path=remote_path)
        os.remove(path=file_path)


