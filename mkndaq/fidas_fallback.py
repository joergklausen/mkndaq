from mkndaq.utils.sftp import SFTPClient
from mkndaq.utils.utils import load_config
from mkndaq.utils.s3fsc import S3FSC
from mkndaq.inst.fidas import FIDAS

def main():
    config = load_config(config_file="dist/mkndaq-fallback.yml")
    name = 'fidas'

    # decide on file transfer mechanism
    s3fsc = None
    sftp = None

    # Prefer S3 when config contains an 's3' section
    if config.get("s3"):
        # You can control these via mkndaq.yml's s3.* or override here if needed
        s3fsc = S3FSC(
            config,
            use_proxies=bool(config["s3"].get("use_proxies", True)),
            addressing_style=config["s3"].get("addressing_style", "path"),
            verify=config["s3"].get("verify", True),
            default_prefix=config["s3"].get("default_prefix", ""),
        )
        print("[s3fsc] client configured")

    if SFTPClient and config.get("sftp"):
        # Optional fallback if S3 is not configured
        sftp = SFTPClient(config=config, name=name)
    else:
        raise RuntimeError("Neither S3 nor SFTP is configured in mkndaq.yml")


    with FIDAS(config=config) as fidas:
        if s3fsc:
            s3fsc.setup_transfer_schedules(
                local_path=fidas.staging_path,
                key_prefix=fidas.remote_path,
                interval=fidas.reporting_interval,
                remove_on_success=False,
            )
            print("[s3fsc] schedules defined")

        if sftp:
            sftp.setup_transfer_schedules(interval=config[name]['reporting_interval'])

        fidas.run()
