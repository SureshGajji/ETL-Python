import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_product_history(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.product_history
   (
     dw_product_id,
     MSRP,
     effective_from_date,
     dw_active_record_ind,
     dw_create_timestamp,
     dw_update_timestamp,
     create_etl_batch_no,
     create_etl_batch_date
    )
    SELECT a.dw_product_id,
       a.MSRP,
       '{ETL_BATCH_DATE}' effective_from_date,
       1 dw_active_record_ind,
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM devdw.products a
    LEFT JOIN devdw.product_history b
         ON a.dw_product_id = b.dw_product_id
         AND b.dw_active_record_ind = 1
    WHERE b.dw_product_id IS NULL;
"""
    sql2 = f"""
   UPDATE devdw.product_history
   SET effective_to_date ='{ETL_BATCH_DATE}',
       dw_active_record_ind = 0,
       dw_update_timestamp = CURRENT_TIMESTAMP,
       update_etl_batch_no ='{ETL_BATCH_NO}',
       update_etl_batch_date='{ETL_BATCH_DATE}'
  from devdw.products p inner join devdw.product_history
    ph on ph.dw_product_id=p.dw_product_id
  AND ph.dw_active_record_ind = 1
   WHERE ph.MSRP <> p.MSRP;
 """
    cur.execute(sql2)
    conn.commit()
    cur.execute(sql1)
    conn.commit()
    print(" Product History data loaded")
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
load_product_history(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
