import mysql.connector as connection
import pandas as pd
from datetime import datetime

from google.cloud import storage

query = """
SELECT
		DATE(date) day,
		a.*, s.name as source_name, b.*, c.*,  d.*,  e.*,  f.name AS str_status
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

conf = dict(
    user = "teamdata_18",
    passwd = "m43{4L%E0sKK3w!fK",
    host = "34.77.19.136",
    port = 3306,
    db = 'db_status',
    use_pure = True,
)

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
    for c in ret.columns:
        c_value = ret[c]
        last_exec = c_value[c_value>0].reset_index().day.max()
        diff = datetime.now()-last_exec
        _ = f'{c} [-{diff.days} days]'
        new_cols.append(_)

    ret.columns = new_cols
    #ret.to_csv('report_execution.csv')
    return ret

if __name__=='__main__':
    ret = main(None)
    print(ret)

