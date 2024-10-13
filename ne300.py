import argparse
import logging
import os
import threading
import time

import schedule

from mkndaq.inst.neph import NEPH
from mkndaq.utils.utils import load_config, setup_logging

"""Read config file, set up instruments, and launch data acquisition."""
# collect and parse CLI arguments
parser = argparse.ArgumentParser(
    description='Data acquisition and transfer for MKN Global GAW Station.',
    usage='python3 mkndaq.py|mkndaq.exe -c [-f]')
parser.add_argument('-c', '--configuration', type=str,
                    help='full path to configuration file',
                    default='C:/Users/mkn/Documents/git/mkndaq/dist/mkndaq.yml', required=False)
parser.add_argument('-f', '--fetch', type=int, default=20,
                    help='interval in seconds to fetch and display current instrument data',
                    required=False)
args = parser.parse_args()
fetch = args.fetch
config_file = args.configuration
# config_file = 'C:/Users/mkn/Documents/git/mkndaq/dist/mkndaq.yml'

# load configuation
cfg = load_config(config_file=config_file)

# setup logging
logfile = os.path.join(os.path.expanduser(str(cfg['root'])),
                        cfg['logging']['file'])
logger = setup_logging(file=logfile)
logger.error('test error logging')

ne300 = NEPH('ne300', cfg, verbosity=0)

def run_threaded(job_func):
    """Set up threading and start job.

    Args:
        job_func ([type]): [description]
    """
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

# limit logging from schedule
logging.getLogger('schedule').setLevel(logging.CRITICAL)

# align start with a 10' timestamp
while int(time.time()) % 10 > 0:
    time.sleep(0.1)

fetch = 40
schedule.every(fetch).seconds.do(ne300.print_ssp_bssp)
schedule.every(cfg['ne300']['sampling_interval']).minutes.at(':10').do(run_threaded, ne300._accumulate_new_data)
# schedule.every(cfg['ne300']['zero_span_check_interval']).minutes.at(':00').do(run_threaded, ne300.do_zero_span_check)
for minute in range(6):
    schedule.every().hour.at(f"{minute}0:01").do(ne300._save_and_stage_data)

while True:
    schedule.run_pending()
    time.sleep(1)