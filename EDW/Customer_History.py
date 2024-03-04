import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_customer_history(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.customer_history
   (
     dw_customer_id,
     creditLimit,
     effective_from_date,
     dw_active_record_ind,
     dw_create_timestamp,
     dw_update_timestamp,
     create_etl_batch_no,
     create_etl_batch_date
    )
    SELECT a.dw_customer_id,
       a.creditlimit,
       '{ETL_BATCH_DATE}' effective_from_date,
       1 dw_active_record_ind,
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM devdw.customers a
    LEFT JOIN devdw.customer_history b
         ON a.dw_customer_id = b.dw_customer_id
        AND b.dw_active_record_ind = 1
    WHERE b.dw_customer_id IS NULL;
"""
    sql2 = f"""
   UPDATE devdw.customer_history
   SET effective_to_date = '{ETL_BATCH_DATE}',
       dw_active_record_ind = 0,
       dw_update_timestamp = CURRENT_TIMESTAMP,
       update_etl_batch_no ='{ETL_BATCH_NO}',
       update_etl_batch_date='{ETL_BATCH_DATE}'
    from devdw.customer_history ch
    INNER JOIN devdw.customers c ON c.dw_customer_id = ch.dw_customer_id
      AND ch.dw_active_record_ind = 1
     WHERE ch.creditlimit <> c.creditlimit;
 """
    cur.execute(sql2)
    conn.commit()
    cur.execute(sql1)
    conn.commit()
    print(" Customer History data loaded")
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
load_customer_history(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
