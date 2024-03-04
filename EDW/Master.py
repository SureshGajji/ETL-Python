import subprocess
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]

python_files = ['Offices.py', 'Employees.py', 'Customers.py',
                'Orders.py', 'ProductLines.py', 'Products.py',
                'OrderDetails.py',  'Payments.py']
files = ['Master_histories.py', 'Master_Customer_Summaries.py',
         'Master_Product_Summaries.py']
x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/EDW"

for file in python_files:
    y = x+"/"+file
    print('path', y)
    subprocess.call(['python', x+"/"+file, ETL_BATCH_NO, ETL_BATCH_DATE])

exe = []
for file in files:
    y = x+"/"+file
    print('path', y)
    exe.append(subprocess.Popen(['python', x+"/"+file, ETL_BATCH_NO,
                                ETL_BATCH_DATE]))
for process in exe:
    process.communicate()
