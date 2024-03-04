import subprocess
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]

files = ['Customer_History.py', 'Product_History.py']

x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/EDW"
exe = []
for file in files:
    y = x+"/"+file
    print('path', y)
    exe.append(subprocess.Popen(['python', x+"/"+file, ETL_BATCH_NO,
                                ETL_BATCH_DATE]))
for process in exe:
    process.communicate()
