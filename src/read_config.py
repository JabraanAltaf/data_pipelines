from configparser import ConfigParser
def read_config(section, option):
    """
    Read a configuration option from the specified section in the config file.

    Parameters:
    - section (str): The section in the configuration file.
    - option (str): The option to retrieve from the specified section.

    Returns:
    str: The value of the specified option.
    """
    config = ConfigParser()
    config.read('../config.ini')
    return config.get(section, option)
