import argparse
import os
import time
import zipfile

from mkndaq.inst.ae33 import AE33
from mkndaq.utils.utils import load_config

if __name__ == "__main__":

    # collect and parse CLI arguments
    parser = argparse.ArgumentParser(
        description='Fetch data records from instrument internal data logger.',
        usage='python3 ae33_fetch_records.py -c')
    parser.add_argument('-c', '--configuration', type=str,
                        help='full path to configuration file',
                        default='dist/mkndaq.yml', required=False)
    parser.add_argument('-r', '--records', type=int,
                        help='number of records to retrieve',
                        default='1440', required=False)
    parser.add_argument('-f', '--first', type=int,
                        help='row id of first record to retrieve',
                        default='525000', required=False)
    args = parser.parse_args()
    cfg = load_config(config_file=args.configuration)

    ae33 = AE33(name='ae33', config=cfg)

    # Define file names/paths
    dtm = time.strftime("%y%m%d%H%M")
    data_file_name = os.path.join(os.path.expanduser(cfg['root']), cfg["data"], "ae33", "data", f"ae33-from-logger-{dtm}.dat")
    zip_file_path = os.path.join(os.path.expanduser(cfg['root']), cfg["staging"], "ae33", "data", f"ae33-from-logger-{dtm}.zip")

    data = ae33._fetch_from_table(name='Data', rows=args.records, first=args.first)
    data = data.replace("AE33>", "")

    # Write raw_data to a text file
    with open(data_file_name, "w") as fh:
        fh.write(data)

    # Create a zip archive
    with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(data_file_name, arcname=os.path.basename(data_file_name))

    print(f"Zipped file saved as {zip_file_path}")
