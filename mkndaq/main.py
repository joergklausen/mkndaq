# -*- coding: utf-8 -*-


import os
# import logging
import yaml
import time

from daqman.tei49c import TEI49C

def config(config_file):
    """
    Read config file.
    
    Read config file.

    Parameters
    ----------
    config_file : str
        full path to yaml config file
        
    Returns
    -------
    configuration information as dictionary
    """
    try:
        with open(os.path.abspath(config_file), "r") as f:
            cfg = yaml.safe_load(f)
            f.close()
        return(cfg)
    
    except Exception as err:
        print('Failed to read config file %s.' % config_file)
        print(err)
    
    
if __name__ == '__main__':
    config_file = os.path.join(
        os.path.dirname(__file__), "config.yaml")

    cfg = config(config_file)
    
    tei49c = TEI49C('tei49c', 'COM2', cfg)
    tei49c.get_config()
    tei49c.set_config()
    
    for i in range(10):
        res = tei49c.get_data(log=True)
        print(res)
        time.sleep(40)
    
    
#        # remove any existing logging handlers in root
#        for h in logging.root.handlers[:]:
#            logging.root.removeHandler(h)
#    
#        # setup logging
#        logdir = os.path.expanduser(cfg['logfile'])
#        os.makedirs(logdir, exist_ok=True)
#        logfile = '%s.log' % time.strftime('%Y%m%d')
#        self.logfile = os.path.join(logdir, logfile)
#        self.logger = logging.getLogger(__name__)
#        logging.basicConfig(level=logging.DEBUG,
#                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
#                            datefmt='%y-%m-%d %H:%M:%S',
#                            filename=str(self.logfile),
#                            filemode='a')
    