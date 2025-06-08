from mkndaq.utils.sftp import SFTPClient
from mkndaq.utils.utils import load_config
from mkndaq.inst.fidas import FIDAS

def main():
    config = load_config(config_file="dist/mkndaq.yml")
    name = 'fidas'

    sftp = SFTPClient(config=config, name=name)
    sftp.setup_transfer_schedules(interval=config[name]['reporting_interval'])


    with FIDAS(config=config) as fidas:
        fidas.run()
