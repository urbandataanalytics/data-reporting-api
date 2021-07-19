from scrapinghub import ScrapinghubClient
import pandas as pd

from pprint import pprint

apikey = 'b959a20caa184e45b176b47071e5c73a'

def main(context):
    client = ScrapinghubClient(apikey)
    dflist = []
    for pid in client.projects.list():
        proj = client.get_project(pid)
        act = [i for i in proj.jobs.iter_last()]
        if len(act):
            part = pd.DataFrame(act)
            dflist.append(part)
    df = pd.concat(dflist)
    df.fillna(0, inplace=True)
    df.eval('err_pct=errors/logs', inplace=True)
    df.query('err_pct>.2', inplace=True)
    return df

if __name__=='__main__':
    ret = main(None)
    print(ret)
