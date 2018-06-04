from configparser import ConfigParser
import io

def read_config(file):
    config = ConfigParser()
    with open(file) as f:
        config.readfp(f)
    return {section: dict(config.items(section)) for section in config.sections()}
