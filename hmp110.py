import os
from mkndaq.utils.utils import (load_config, setup_logging)

config_file = "dist/mkndaq.yml"

def main():
    # load configuation
    cfg = load_config(config_file=config_file)

    # setup logging
    logfile = os.path.join(os.path.expanduser(str(cfg['root'])),
                        cfg['logging']['file'])
    logger = setup_logging(file=logfile, loglevel_console=cfg['logging']['level_console'],
                           loglevel_file=cfg['logging']['level_file'])

    try:
        if cfg.get('hmp110-inlet', None):
            from mkndaq.inst.vaisala import HMP110ASCII
            hmp110_inlet = HMP110ASCII(name='hmp110-inlet', config=cfg)
            cmd = f"SEND {hmp110_inlet._id}\r\n"
            response = hmp110_inlet.serial_comm(cmd)
            logger.info(repr(response))

        if cfg.get('hmp110-ae33', None):
            from mkndaq.inst.vaisala import HMP110ASCII
            hmp110_ae33 = HMP110ASCII(name='hmp110-ae33', config=cfg)
            cmd = f"SEND {hmp110_ae33._id}\r\n"
            response = hmp110_ae33.serial_comm(cmd)
            logger.info(repr(response))

    except Exception as err:
        logger.error(err)

if __name__ == "__main__":
    main()