"""test."""
import paramiko
import os

from paramiko.sftp_client import SFTP

try:
    paramiko.util.log_to_file("paramiko.log")

    host = 'sftp.meteoswiss.ch'
    usr = 'gaw_mkn'
    key = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~/.ssh/private-open-ssh-4096-mkn.ppk'))

    localpath = os.path.expanduser('~/Public/git/mkndaq/mkndaq/tests/data/')
    remotepath = './test/'

    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=usr, pkey=key)
        
        with ssh.open_sftp() as sftp:

            # determine local directory structure, establish same structure on remote host
            for dirpath, dirnames, filenames in os.walk(top=localpath):
                dirpath = dirpath.replace(localpath, remotepath)
                try:
                    print(dirpath)
                    sftp.mkdir(dirpath, mode=16877)
                    print(sftp.lstat(dirpath).st_mode)
                except OSError:
                    pass

            # walk local directory structure, put file to remote location
            for dirpath, dirnames, filenames in os.walk(top=localpath):
                for filename in filenames:
                    localitem = os.path.join(dirpath, filename)
                    print(localitem)
                    remoteitem = os.path.join(dirpath.replace(localpath, remotepath), filename) 
                    print(remoteitem)
                    sftp.put(localpath=localitem, remotepath=remoteitem, confirm=True)

            sftp.close()

except Exception as err:
    print(err)
    sftp.close()


# try:
#     paramiko.util.log_to_file("paramiko.log")

#     host = 'sftp.meteoswiss.ch'
#     usr = 'gaw_mkn'
#     key = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~/.ssh/private-open-ssh-4096-mkn.ppk'))

#     localpath = os.path.expanduser('~/Public/git/mkndaq/mkndaq/tests/data/sftptest')
#     remotepath = './sftptest'

#     # with paramiko.SSHClient() as ssh:
#     #     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     #     ssh.connect(hostname=host, username=usr, pkey=key)
        
#     #     with ssh.open_sftp() as sftp:
#     #         uid = sftp.stat('.').st_uid
#     #         # sftp.put(localpath=localpath, remotepath=remotepath)
#     #         sftp.close()
#     #     ssh.connect()
#     # print(uid)

# except Exception as err:
#     print(err)
#     # sftp.close()
