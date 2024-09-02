import os
import logging
import yaml

def load_config(config_file: str) -> dict:
    """
    Load configuration from config file.

    :param config_file: Path to the configuration file.
    :return: dict.
    """
    config = dict()
    try:
        extension = os.path.basename(config_file).split(".")[1].lower()

        if extension in ['yaml', 'yml', 'cfg']:
            with open(config_file, 'r') as fh:
                config = yaml.safe_load(fh)
        else:
            print("Extension of config file not recognized!)")
        return config
    except Exception as err:
        print(err)
        return config

def setup_logging(file: str) -> logging.Logger:
    """Setup the main logging device

    Args:
        file (str): full path to log file

    Returns:
        logging: a logger object
    """
    file_path = os.path.dirname(file)
    main_logger = os.path.basename(file).split('.')[0]
    logger = logging.getLogger(main_logger)
    try:
        os.makedirs(file_path, exist_ok=True)

        logger.setLevel(logging.DEBUG)

        # create file handler which logs warning and above messages
        fh = logging.FileHandler(file)
        fh.setLevel(logging.WARNING)

        # create console handler which logs even debugging information
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(name)s, %(message)s', datefmt="%Y-%m-%dT%H:%M:%S")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger
    except Exception as err:
        print(err)
        return logger
