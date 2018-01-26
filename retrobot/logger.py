import logging

class Logger(object):
    """
        Set up logger.
    """

    def __init__(self, log_file):
        self.log_file = log_file
        self.configure_logging()
    
    def configure_logging(self):
        """
            Configure logging.
        """
        logging.basicConfig(
            filename=self.log_file, 
            filemode='w', 
            format='%(asctime)s:%(levelname)s:%(message)s'
        )

    def log(self, level, **kwargs):
        """
            Log event.
        """
        message = " ".join(
            ['{}:{}'.format(str(key), str(value)) for key, value in kwargs.iteritems()]
        )

        if level == 'debug':
            logging.debug(message)

        elif level == 'info':
            logging.info(message)

        elif level == 'warn':
            logging.info(message)

        elif level == 'error':
            logging.error(message)

        elif level == 'critical':
            logging.critical(message)

        else:
            raise Exception('Incorrect log level.')
