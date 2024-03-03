import subprocess
import psycopg2

conn = psycopg2.connect(
        dbname='dev',
        user='admin',
        password='Surya183',
        host=('default-workgroup.278921652245.us-east-1.'
              'redshift-serverless.amazonaws.com'),
        port='5439'
    )
print("Connected to Redshift successfully")
cursor = conn.cursor()
cursor.execute("Truncate table devstage.customers;")
cursor.execute("Truncate table devstage.employees;")
cursor.execute("Truncate table devstage.offices;")
cursor.execute("Truncate table devstage.orderdetails;")
cursor.execute("Truncate table devstage.orders;")
cursor.execute("Truncate table devstage.payments;")
cursor.execute("Truncate table devstage.productlines;")
cursor.execute("Truncate table devstage.products;")
print("Truncated successfully")
cursor.close()
python_files = ['Customers.py', 'Employees.py', 'Offices.py',
                'OrderDetails.py', 'Orders.py', 'Payments.py',
                'ProductLines.py', 'Products.py']
x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/S3_to_Stage"
exe = []
for file in python_files:
    y = x+"/"+file
    print('path', y)
    exe.append(subprocess.Popen(['python', x+"/"+file]))
for process in exe:
    process.communicate()
print("All tables data loaded successfully")
