import argparse
import os
import time
import zipfile

from mkndaq.inst.ae33 import AE33
from mkndaq.utils.utils import load_config, setup_logging

cfg = {"data": "~/Documents/mkndaq/data",
        "reporting_interval": 10,
        "staging": {
            "path": "~/Documents/mkndaq/staging",
        },
        "logging": {
            "file": "ae33.log",
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
                 "reporting_interval": 10,
                 "sampling_interval": 1,        # minutes. How often should data be requested from instrument?
                 "staging_zip": True,
                 "MAC": "C4-00-AD-D8-0A-AC"
            }
       }

if __name__ == "__main__":

    # collect and parse CLI arguments
    parser = argparse.ArgumentParser(
        description='Fetch data records from instrument internal data logger.',
        usage='python3 ae33_download_from_logger.py -c')
    parser.add_argument('-c', '--configuration', type=str,
                        help='full path to configuration file',
                        default='dist/mkndaq.yml', required=False)
    parser.add_argument('-r', '--records', type=int,
                        help='number of records to retrieve',
                        default='1440', required=False)
    parser.add_argument('-f', '--first', type=int,
                        help='number of records to retrieve',
                        default='525000', required=False)
    parser.add_argument('-l', '--last', type=int,
                        help='number of records to retrieve',
                        default='526000', required=False)
    args = parser.parse_args()
    cfg = load_config(config_file=args.configuration)

    ae33 = AE33(name='ae33', config=cfg)

    # Define file names/paths
    dtm = time.strftime("%y%m%d%H%M")
    data_file_name = os.path.join(os.path.expanduser(cfg['root']), cfg["data"], "ae33", "data", f"ae33-from-logger-{dtm}.dat")
    zip_file_path = os.path.join(os.path.expanduser(cfg['root']), cfg["staging"], "ae33", "data", f"ae33-from-logger-{dtm}.zip")

    data = ae33._fetch_from_table(name='Data', rows=args.records)
    data = data.replace("AE33>", "")


    # Write raw_data to a text file
    with open(data_file_name, "w") as fh:
        fh.write(data)  # Writing the extracted data

    # Create a zip archive
    with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(data_file_name, arcname=data_file_name)

    print(f"Zipped file saved as {zip_file_path}")

    # remove .pickle and .dat files
    # os.remove(data_file_name)