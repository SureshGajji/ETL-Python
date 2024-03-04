import oracledb
import subprocess
import sys


clientpath = ("C:/Users/Suresh.gajji/Downloads/instantclient-basic-"
              "windows.x64-19.22.0.0.0dbru/instantclient_19_22")
oracledb.init_oracle_client(lib_dir=clientpath)

# Oracle Credentails
username = 'f23suresh'
password = 'f23suresh123'
dsn = '54.224.209.13:1521/xe'

ETL_BATCH_DATE = sys.argv[2]

schema = sys.argv[3]
schema_password = sys.argv[4]

connection = oracledb.connect(user=username, password=password, dsn=dsn)
cursor = connection.cursor()
drop_dblink = "DROP PUBLIC DATABASE LINK f23suresh_dblink_classicmodels"
cursor.execute(drop_dblink)
print("DB Link Dropped")
create_dblink = ("CREATE PUBLIC DATABASE LINK f23suresh_dblink_classicmodels "
                 f"CONNECT TO {schema} identified by {schema_password} using "
                 "'XE'")
cursor.execute(create_dblink)
print("DB Link Created Successfully")
cursor.close()
connection.close()

python_files = ['Customers.py', 'Employees.py', 'Offices.py',
                'OrderDetails.py', 'Orders.py', 'Payments.py',
                'ProductLines.py', 'Products.py']
x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/Oracle_to_S3"
exe = []
for file in python_files:
    y = x+"/"+file
    print('path', y)
    exe.append(subprocess.Popen(['python', x+"/"+file, ETL_BATCH_DATE]))
for process in exe:
    process.communicate()
