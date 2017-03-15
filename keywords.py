import luigi
import pandas as pd
from content import RetrieveNewsContentTask
import jieba
from os.path import join as join_path
DATA_DIR = "data/jieba"
jieba.load_userdict(join_path(DATA_DIR, "dict.txt.big"))
import jieba.analyse
jieba.analyse.set_idf_path(join_path(DATA_DIR, "idf.txt.big"))
jieba.analyse.set_stop_words(join_path(DATA_DIR, "stop_words.txt"))

import jieba.posseg as pseg

class RetrieveNewsKeywordsTask(luigi.Task):
    def requires(self):
        return [RetrieveNewsContentTask()]
    def output(self):
        return luigi.LocalTarget("keywords_urls.csv")

    def get_keywords(self, content):
        result = pseg.cut(content)
        tags = jieba.analyse.textrank(content, topK=50, withWeight=False, allowPOS=('n'))
        tags = [tag for tag in tags if len(tag) > 2]
        return tags 

    def run(self):
        df = pd.read_json('contents.json')
        output = []
        for index, row in df.iterrows():
            tags = self.get_keywords(row['content'])
            for tag in tags:
                output.append({'url': row['url'], 'keyword': tag.encode('utf-8')})
        df2 = pd.DataFrame(output)
        with self.output().open('w') as f:
            df2.to_csv(f, index=False)

