import configparser
import logbook

class stockpredictor:
    def __init__(self):
        """
        Check if we meet all necessary requriements (L{check_reqs}), set up
        config file and logging.
        """
        self.check_reqs()
        self.config = configparser.ConfigParser()
        self.config.read(os.getcwd()+"/stockpredictor.ini")
        self.log = logbook.Logger('Logbook')
