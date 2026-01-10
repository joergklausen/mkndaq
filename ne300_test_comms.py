# always run first
from mkndaq.inst.neph import NEPH
from mkndaq.utils.utils import load_config

# path to configuration file
config_file = 'C:/Users/mkn/Documents/git/mkndaq/dist/mkndaq.yml'

# load configuation
cfg = load_config(config_file=config_file)

# # Initialize NEPH
ne300 = NEPH('ne300', cfg, verbosity=2)
#   Instrument identified itself as '{'Model': 158, 'Variant': 300, 'Sub-Type': 0, 'Range': 0, 'Build': 158, 'Branch': 300}'.

# res = ne300.get_id()
# print(res)

# get setting of data logging target (0: SD card, 1: USB Flash drive)
res = ne300.get_values([2200], verbosity=2)
print(f"DATALOG_MEDIA: {res}")
# if res[2200]==1:
#     ne300.set_value(parameter_id=2200, value=0)
#     print(f"(new) DATALOG_MEDIA: {ne300.get_values([2200], verbosity=2)}")

# get current operation
print(f"CURRENT_OPERATION: {ne300.get_values([4035], verbosity=2)}")

# get current state
print(f"CURRENT_STATE: {ne300.get_values([4036], verbosity=2)}")

# get data logging interval
print(f"DATALOG_PARAMETER_INTERVAL: {ne300.get_values([2001], verbosity=2)}")

# get data logging interval
print(f"DATALOG_PARAMETER_INTERVAL_SECONDS: {ne300.get_values([2002], verbosity=2)}")