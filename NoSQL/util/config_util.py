import configparser

class ConfigUtility():
    def __init__(self, filepath):
        self.filepath = filepath
        self.config = configparser.ConfigParser()
        self.config.read(self.filepath)
        return

    def get_config(self):
        return self.config

