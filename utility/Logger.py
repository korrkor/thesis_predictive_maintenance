class Logger:
    def __init__(self, prefix):
        self.prefix = prefix

    def info(self, log_string):
        assert isinstance(log_string, str),\
            'String passed for logging is not an instance of string'
        print self.prefix + " INFO: " + log_string

    def success(self, log_string):
        assert isinstance(log_string, str),\
            'String passed for logging is not an instance of string'
        print self.prefix + self.green(" SUCCESS: ") + log_string

    def warn(self, log_string):
        assert isinstance(log_string, str),\
            'String passed for logging is not an instance of string'
        print self.prefix + self.yellow(" WARN: ") + log_string

    def error(self, log_string):
        assert isinstance(log_string, str),\
            'String passed for logging is not an instance of string'
        print self.prefix + self.red(" ERROR: ") + log_string

    def green(self, log_string):
        return '\033[32m' + log_string + '\033[0m'

    def yellow(self, log_string):
        return '\033[33m' + log_string + '\033[0m'

    def red(self, log_string):
        return '\033[31m' + log_string + '\033[0m'
