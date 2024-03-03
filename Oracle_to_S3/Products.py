import csv
import oracledb
import boto3
import sys
from io import StringIO

clientpath = ("C:/Users/Suresh.gajji/Downloads/instantclient-basic-"
              "windows.x64-19.22.0.0.0dbru/instantclient_19_22")
oracledb.init_oracle_client(lib_dir=clientpath)

# Oracle Credentails
username = 'f23suresh'
password = 'f23suresh123'
dsn = '54.224.209.13:1521/xe'

ETL_BATCH_DATE = sys.argv[1]

region_name = 'us-east-1'
s3_bucket_name = 'etlpython'
s3_key = 'Products.csv'

try:
    connection = oracledb.connect(user=username, password=password, dsn=dsn)
    cursor = connection.cursor()
    sql_query = ("SELECT PRODUCTCODE"
                 ", PRODUCTNAME"
                 ", PRODUCTLINE"
                 ", PRODUCTSCALE"
                 ", PRODUCTVENDOR"
                 ", QUANTITYINSTOCK"
                 ", BUYPRICE"
                 ", MSRP"
                 ", CREATE_TIMESTAMP"
                 ", UPDATE_TIMESTAMP"
                 " from Products@f23suresh_dblink_classicmodels"
                 " WHERE to_char(UPDATE_TIMESTAMP,'yyyy-mm-dd')"
                 f">='{ETL_BATCH_DATE}'")
    cursor.execute(sql_query)
    result = cursor.fetchall()
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow([i[0] for i in cursor.description])
    csv_writer.writerows(result)

    cursor.close()
    connection.close()

    s3 = boto3.client(
        's3', region_name=region_name
        )
    s3.put_object(
        Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue()
        )

    print("Products CSV file uploaded to S3 successfully.")

except oracledb.Error as error:
    print('Error occurred:', error)

except Exception as e:
    print('Error occurred:', e)
