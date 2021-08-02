# -*- coding: utf-8 -*-

import os
import yaml


def config(file=os.path.join(os.pardir, "mkndaq.cfg")) -> dict:
    """
    Read config file.

    :param file: full path to yaml config file
    :return: configuration information
    """
    try:
        print("# Read configuration from %s:" % os.path.abspath(file))
        with open(os.path.abspath(file), "r") as fh:
            cfg = yaml.safe_load(fh)
            fh.close()

        return cfg

    except Exception as err:
        print(err)


if __name__ == "__main__":
    print(config())
