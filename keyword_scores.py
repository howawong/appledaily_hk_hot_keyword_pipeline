import pandas as pd
import luigi
from likes import RetrieveNewsLikeCountsTask
from keywords import RetrieveNewsKeywordsTask
from config import KEYWORDS_SCORE_CSV, LIKE_COUNTS_CSV, KEYWORDS_URLS_CSV
class RetrieveNewsKeywordsScoresTask(luigi.Task):
    def requires(self):
        return [RetrieveNewsKeywordsTask(), RetrieveNewsLikeCountsTask()]
    def output(self):
        return luigi.LocalTarget(KEYWORDS_SCORE_CSV)
    def run(self):
        counts_df = pd.read_csv(LIKE_COUNTS_CSV) 
        keywords_df = pd.read_csv(KEYWORDS_URLS_CSV)
        combined_df = keywords_df.join(counts_df.set_index('url'), on='url')
        combined_df = combined_df[['keyword','count']]
        output_df = combined_df.groupby('keyword').sum()
        output_df['keyword'] = output_df.index
        output_df = output_df.sort_values('count', ascending=False)
        with self.output().open('w') as f:
            output_df.to_csv(f, index=False)


