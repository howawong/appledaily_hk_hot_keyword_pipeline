import luigi
import logging
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from config import LINKS_CSV

class RetrieveNewsLinksTask(luigi.Task):
    def requires(self):
        return []
    def output(self):
        return luigi.LocalTarget(LINKS_CSV)

    def get_links(self):
        d = '20170313'
        url = 'http://hk.apple.nextmedia.com/catalog/index/' + d
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href is not None and d in href and 'index' not in href:
                links.append(href)
        return list(set(links))


    def run(self):
        logging.info("Start fetching news links")
        with self.output().open('w') as f:
            links = self.get_links()
            df = pd.DataFrame(links, columns=['url'])
            df.to_csv(f, index=False)


