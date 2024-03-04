import subprocess

ETL_BATCH_NO = '1006'
ETL_BATCH_DATE = '2005-06-14'

schema = 'cm_20050614'
schema_password = 'cm_20050614123'

python_files = ['Oracle_to_S3/Master.py',
                'S3_to_Stage/Master.py',
                'EDW/Master.py']
x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/"

for file in python_files:
    y = x+"/"+file
    print('path', y)
    subprocess.call(['python', x+"/"+file, ETL_BATCH_NO, ETL_BATCH_DATE,
                    schema, schema_password])
