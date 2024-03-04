import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_productlines(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.productlines
  (
   productLine           ,
   src_create_timestamp,
   src_update_timestamp,
   etl_batch_no        ,
   etl_batch_date
  )
 SELECT
  p.PRODUCTLINE
, p.create_timestamp
, p.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM devstage.productlines p left join devdw.productlines p1
on p.productLine=p1.productLine
where p1.productLine is null;
"""
    sql2 = f"""
  update devdw.productlines a
  set
    productLine            =b.productline       ,
    src_update_timestamp   =b.update_timestamp  ,
    dw_update_timestamp    =CURRENT_TIMESTAMP   ,
    etl_batch_no           ='{ETL_BATCH_NO}'    ,
    etl_batch_date        ='{ETL_BATCH_DATE}'
   from devstage.productlines b
  where a.productline=b.productline;
    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Productlines data loaded")
    conn.commit()
    conn.close()


if __name__ == "__main__":

    redshift_params = {
        'dbname': 'dev',
        'user': 'admin',
        'password': 'Surya183',
        'host': ('default-workgroup.278921652245.us-east-1.'
                 'redshift-serverless.amazonaws.com'),
        'port': '5439'
    }
load_productlines(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
