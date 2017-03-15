import luigi
import logging
import requests
from urllib import urlencode
import pandas as pd
import time
from apple_links import RetrieveNewsLinksTask
from config import LIKE_COUNTS_CSV, LINKS_CSV, ACCESS_TOKEN
class RetrieveNewsLikeCountsTask(luigi.Task):
    def requires(self):
        return [RetrieveNewsLinksTask()]
    
    def output(self):
        return luigi.LocalTarget(LIKE_COUNTS_CSV)

    def chunks(self, l, n):
        output = []
        for i in range(0, len(l), n):
            output.append(l[i:i + n])
        print output
        return output

    def get_like_count_by_url(self, id_urls):
        d = {}
        #TODO: put in the token
        for sub_list in self.chunks(id_urls, 3):
            print "Processing" + ",".join(sub_list)
            url = "https://graph.facebook.com/?%s" % (urlencode({'ids': ",".join(sub_list), 'fields': 'og_object{engagement{count}}', 'access_token': ACCESS_TOKEN}))
            r = requests.get(url)
            o = r.json()
            print o
            d.update(o)
            if "error" in o:
                raise Exception("Too many Requests")
            time.sleep(3)
        print d.keys()
        counts = {}
        for k in d.keys():
            o = d[k]
            oj = o.get("og_object", {"og_object": {"engagement": {"count": 0}}})
            e = oj.get("engagement", {"count": 0})
            counts[k] = e["count"]
        return counts
     

    def run(self):
        df = pd.read_csv(LINKS_CSV)
        counts = self.get_like_count_by_url(df['url'].tolist())
        df['count'] = df['url'].map(counts)
        with self.output().open('w') as f:
            df.to_csv(f, index=False)


