import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_orders(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.orders
  (
    dw_customer_id        ,
    src_orderNumber       ,
    orderDate             ,
    requiredDate          ,
    shippedDate           ,
    status                ,
    src_customerNumber    ,
    src_create_timestamp  ,
    src_update_timestamp  ,
    etl_batch_no          ,
    etl_batch_date        ,
    cancelleddate
  )
  SELECT
    c.dw_customer_id
  , o.ORDERNUMBER
  , o.ORDERDATE
  , o.REQUIREDDATE
  , o.SHIPPEDDATE
  , o.STATUS
  , o.CUSTOMERNUMBER
  , o.create_timestamp
  , o.update_timestamp
  ,'{ETL_BATCH_NO}'
  ,'{ETL_BATCH_DATE}'
  , o.cancelleddate
  FROM devstage.Orders o left join devdw.orders o1
  on o.orderNumber=o1.src_orderNumber
  inner join devdw.customers c
  on o.customernumber=c.src_customernumber
  where o1.src_orderNumber is null;

  """
    sql2 = f"""
  update devdw.orders a
  set
     orderDate            =b.orderdate             ,
     requiredDate         =b.requireddate          ,
     shippedDate          =b.shippeddate           ,
     status               =b.status                ,
     src_customerNumber   =b.customernumber        ,
     src_update_timestamp =b.update_timestamp      ,
     etl_batch_no         ='{ETL_BATCH_NO}'        ,
     etl_batch_date       ='{ETL_BATCH_DATE}'      ,
     cancelleddate        =b.cancelleddate         ,
     dw_update_timestamp  =CURRENT_TIMESTAMP
     from devstage.Orders b
  where a.src_orderNumber  =b.ordernumber;
    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Orders data loaded")
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
load_orders(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
