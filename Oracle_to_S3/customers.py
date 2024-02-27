import csv
import oracledb
import boto3
from io import StringIO

clientpath = "C:/Users/Suresh.gajji/Downloads/instantclient-basic-windows.x64-19.22.0.0.0dbru/instantclient_19_22"
oracledb.init_oracle_client(lib_dir=clientpath)

# Oracle Credentails
username = 'f23suresh'
password = 'f23suresh123'
dsn = '54.224.209.13:1521/xe'

# AWS Credentails
aws_access_key_id = 'AKIAUB4IGVAKT7ML4OWR'
aws_secret_access_key = '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'
region_name = 'us-east-1'
s3_bucket_name = 'etlpython'
s3_key = 'Customers.csv'

try:
    connection = oracledb.connect(user=username, password=password, dsn=dsn)
    cursor = connection.cursor()
    sql_query = "SELECT * from classicmodels.Customers"
    cursor.execute(sql_query)
    result = cursor.fetchall()
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow([i[0] for i in cursor.description])
    csv_writer.writerows(result)

    cursor.close()
    connection.close()

    s3 = boto3.client(
        's3', aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key, region_name=region_name
        )
    s3.put_object(
        Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue()
        )

    print("Customers CSV file uploaded to S3 successfully.")

except oracledb.Error as error:
    print('Error occurred:', error)

except Exception as e:
    print('Error occurred:', e)
