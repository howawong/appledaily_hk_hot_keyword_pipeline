import luigi
import logging
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from apple_links import RetrieveNewsLinksTask
from content import RetrieveNewsContentTask
from keywords import RetrieveNewsKeywordsTask
from likes import RetrieveNewsLikeCountsTask
from keyword_scores import RetrieveNewsKeywordsScoresTask
   

class Workflow(luigi.Task):
    def requires(self):
        yield RetrieveNewsKeywordsScoresTask()
        yield RetrieveNewsKeywordsTask()
        yield RetrieveNewsLinksTask()
        yield RetrieveNewsLikeCountsTask()
        yield RetrieveNewsContentTask()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    luigi.interface.setup_interface_logging()
    luigi.run()
