import mysql.connector as connection
import pandas as pd
from datetime import datetime
import os

from google.cloud import storage

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from slack_sdk.webhook import WebhookClient
url = "https://hooks.slack.com/services/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"
#url = "https://hooks.slack.com/services/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"
webhook = WebhookClient(url)

PASSWD = os.getenv('UDA_PASSWD', 'm43{4L%E0sKK3w!fK') # change
USER = os.getenv('UDA_USER', 'teamdata_18')
HOST = os.getenv('UDA_HOST', "34.77.19.136")
BUCKET = os.getenv('BUCKET', "uda-reporting-data-etl")

conf = dict(
    user = USER,
    passwd = PASSWD,
    host = HOST,
    port = 3306,
    db = 'db_status',
    use_pure = True,
)

query = """
SELECT
    DATE(date) day,
    a.*,
    s.name as source_name,
    b.*, c.*,  d.*,  e.*,
    f.name AS str_status
    FROM db_status.st_execution_log a
        LEFT JOIN db_etl.etl_stage_execution b
            ON a.id_execution=b.id_execution
        LEFT JOIN configurations.co_source s
            ON s.id_source = a.id_source
        LEFT JOIN db_etl.etl_stage c
            ON b.id_stage=c.id_stage
        LEFT JOIN db_data_acquisition.da_loaded_acquisition d
            ON d.id_load=b.id_load
        LEFT JOIN db_data_acquisition.da_data_acquisition e
            ON e.id_acquisition=d.id_acquisition
        LEFT JOIN db_data_acquisition.da_status_acquisition f
            ON f.id_status=e.id_status
    ORDER BY day DESC
"""

mydb = connection.connect(**conf)

def main(context):
    df = pd.read_sql(query, mydb)
    df.day = pd.to_datetime(df.day)

    names = {'Acquisition': '1. Acquisition' ,
     'Data Homogenizer': '3. Data Homogenizer',
     'Master Data Writer': '5. Master Data Writer',
     None: '--',
     'Schema Homogenizer': '2. Schema Homogenizer',
     'Spatial Join Geometry': '4. Spatial Join Geometry'}

    df['name_sort'] = df.name.map(names)

    # assigning new id_jenkins to first step
    s = 'SCHEMA_HOMO'
    map_ids = df.query('build_url==@s')[['id_jenkins', 'input']].set_index('input').id_jenkins.to_dict()
    df.loc[df.build_url == 'sources_gathering', 'id_jenkins'] = df.loc[df.build_url == 'sources_gathering', 'output'].map(map_ids)

    df.dropna(subset=['day'], inplace=True)
    df['year'] = df.day.dt.year.map(int)
    df['week'] = df.day.dt.isocalendar().week.map(int)

    df['i'] = 1
    df2 = df.groupby(['year', 'week', 'day', 'source_name', 'name_sort', 'str_status']).count().i
    ret = df2.unstack(['source_name']).fillna(0).sort_values(['day', 'name_sort'], ascending=[False, True])

    new_cols = []
    sort_cols = []
    for c in ret.columns:
        c_value = ret[c]
        last_exec = c_value[c_value>0].reset_index().day.max()
        diff = datetime.now()-last_exec
        _ = f'{c} [-{diff.days} days]'
        new_cols.append(_)
        sort_cols.append((_, diff.days))

    sort_cols.sort(key=lambda tup: tup[1], reverse=True)  # sorts in place

    ret.columns = new_cols
    ret = ret[[c for c,d in sort_cols]]
    public_url = export(ret, BUCKET)

    msg = "Hi, don't worry about me! I'm a learning bot printing report ***bzzZZt*** messages.\n\n"
    msg += "\nTake a look at what I've found about data-factory's latest executions: \n\n"

    for c,d in sort_cols:
        add = ':fire:'*int(d/7)
        msg += f'    {c} {add} \n'

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

    return public_url

def export(df, bucket):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    object_ = 'public/execution_report.csv'
    blob = bucket.blob(object_)
    blob.cache_control = 'no-store'
    blob.patch()
    blob.upload_from_string(df.to_csv(), 'text/csv')
    public_url = f'https://storage.cloud.google.com/{BUCKET}/{object_}'
    return public_url

if __name__=='__main__':
    ret = main(None)
    print(ret)

