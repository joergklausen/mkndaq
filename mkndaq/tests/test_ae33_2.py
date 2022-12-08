# %%
import os
os.chdir("c:/users/jkl/Public/git/mkndaq")
from mkndaq.inst.ae33 import AE33
import schedule
import time

cfg = {"logs": "c:/users/jkl/Documents/mkndaq/logs",
        "data": "c:/users/jkl/Documents/mkndaq/data",
        "reporting_interval": 10,
        "staging": {
            "path": "c:/users/jkl/Documents/mkndaq/staging",
        },
        "ae33": {"type": "AE33",
                 "id": 169,
                 "serial_number": "AE33-S10-01394",
                 "socket": {"host": "192.168.3.137",
                            "port": 8002,
                            "timeout": 0.5,
                            "sleep": 0.5},
                 "get_config": ["HELLO"],
                 "set_config": [],
                 "set_datetime": False,          # Should date and time be set when initializing the instrument?
                 "sampling_interval": 1,        # minutes. How often should data be requested from instrument?
                 "staging_zip": True,
                 "MAC": "C4-00-AD-D8-0A-AC"
            }
       }

ae33 = AE33(name='ae33', config=cfg)

# %%
remaining = ae33.tape_advances_remaining()
print(f"Tape remaining: {remaining}")

# %%
# logmin = ae33.tcpip_comm(cmd="MINID Log", tidy=False)
# logmax = ae33.tcpip_comm(cmd="MAXID Log", tidy=False)
# log = ae33.tcpip_comm(cmd=f"FETCH Log {logmin} {logmax}", tidy=False)
# cmd, log = ae33.fetch_from_table(name="Log", first="1")
# print(log)
# with open("log.log", "w") as fh:
#     fh.write(log)

# %%
log = ae33.get_new_log_entries()
print(log)

# %%
fetch = 30
schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_data)
schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_log_entries)
schedule.every(fetch).seconds.do(ae33.print_ae33)

print("# Begin data acquisition and file transfer")
while True:
    schedule.run_pending()
    time.sleep(1)


# %%
