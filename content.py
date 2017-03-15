import luigi
from apple_links import RetrieveNewsLinksTask
from goose import Goose
from goose.text import StopWordsChinese
import pandas as pd
from config import  LINKS_CSV, CONTENTS_JSON
class RetrieveNewsContentTask(luigi.Task):
    def requires(self):
        return [RetrieveNewsLinksTask()]

    def output(self):
        return luigi.LocalTarget(CONTENTS_JSON)

    def run(self):
        df = pd.read_csv(LINKS_CSV)
        g = Goose({'stopwords_class': StopWordsChinese})
        df['content'] = df['url'].apply(lambda x: g.extract(url=x).cleaned_text)
        with self.output().open('w') as f:
            df.to_json(f, orient='records')


