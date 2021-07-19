from scrapinghub import ScrapinghubClient
import pandas as pd

import ssl
from slack_sdk.webhook import WebhookClient

apikey = 'b959a20caa184e45b176b47071e5c73a'

ssl._create_default_https_context = ssl._create_unverified_context

url = 'https://hooks.slack.com/services/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'
webhook = WebhookClient(url)

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
    df.query('err_pct>.1', inplace=True)

    df.sort_values('err_pct', ascending=False, inplace=True)

    msg = "Hi, don't worry about me! I'm a learning bot printing report ***bzzZZt*** messages.\n\n"
    msg += "\nTake a look at what I've found about data-factory's latest crawlers errors: \n\n"


    for spider, error, dt in zip(df.spider, df.err_pct, df.running_time):
        error = round(error*100, 2)
        #dt = pd.Timestamp(dt)
        alert = ':fire:'*int(error/20)
        msg += f'- {spider} {alert}: \n\t {error}%\n\n'

    print(msg)

    response = webhook.send(
        text="execution report: ETAs",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": msg
                }
            }
        ]
    )

    assert response.status_code == 200
    assert response.body == "ok"

    return df

if __name__=='__main__':
    ret = main(None)
    print(ret)
