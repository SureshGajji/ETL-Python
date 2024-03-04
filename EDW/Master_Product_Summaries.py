import subprocess
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]

files = ['Daily_Product_Summary.py',
         'Monthly_Product_Summary.py']

x = "C:/Users/Suresh.gajji/Desktop/ETL Python/ETL/ETL-Python/EDW"
for file in files:
    y = x+"/"+file
    print('path', y)
    subprocess.call(['python', x+"/"+file, ETL_BATCH_NO, ETL_BATCH_DATE])
