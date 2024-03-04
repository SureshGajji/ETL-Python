import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_payments(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   insert into devdw.payments
(
   dw_customer_id        ,
   src_customerNumber    ,
   checkNumber           ,
   paymentDate           ,
   amount                ,
   src_create_timestamp  ,
   src_update_timestamp  ,
   etl_batch_no          ,
   etl_batch_date
)
SELECT
  c.dw_customer_id
, p.CUSTOMERNUMBER
, p.CHECKNUMBER
, p.PAYMENTDATE
, p.AMOUNT
, p.create_timestamp
, p.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM devstage.Payments p left join devdw.payments
 p1 on p.checkNumber=p1.checkNumber
inner join devdw.customers c on c.src_customernumber=p.customernumber
where p1.checkNumber is null;
"""
    sql2 = f"""
  update devdw.payments a
  set
     checkNumber         =b.checknumber,
     paymentDate         =b.paymentdate,
     amount              =b.amount,
     src_update_timestamp=b.update_timestamp,
     dw_update_timestamp=current_timestamp,
     etl_batch_no          ='{ETL_BATCH_NO}',
     etl_batch_date        ='{ETL_BATCH_DATE}'
     from devstage.Payments b
  where a.checkNumber=b.checkNumber;

    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Payments data loaded")
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
load_payments(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
