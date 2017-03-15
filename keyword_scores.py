import pandas as pd
import luigi
from likes import RetrieveNewsLikeCountsTask
from keywords import RetrieveNewsKeywordsTask
class RetrieveNewsKeywordsScoresTask(luigi.Task):
    def requires(self):
        return [RetrieveNewsKeywordsTask(), RetrieveNewsLikeCountsTask()]
    def output(self):
        return luigi.LocalTarget("keywords_scores.csv")
    def run(self):
        counts_df = pd.read_csv('like_counts.csv') 
        keywords_df = pd.read_csv('keywords_urls.csv')
        combined_df = keywords_df.join(counts_df.set_index('url'), on='url')
        combined_df = combined_df[['keyword','count']]
        output_df = combined_df.groupby('keyword').sum()
        output_df['keyword'] = output_df.index
        output_df = output_df.sort_values('count', ascending=False)
        with self.output().open('w') as f:
            output_df.to_csv(f, index=False)


