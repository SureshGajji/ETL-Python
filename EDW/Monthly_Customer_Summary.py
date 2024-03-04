import psycopg2

ETL_BATCH_NO = 1001
ETL_BATCH_DATE = '2001-01-01'


def load_monthly_customer_smy(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   DELETE FROM devdw.monthly_customer_summary
   WHERE start_of_the_month_date >=
     date_trunc('month', '{ETL_BATCH_DATE}'::date);
   """
    sql2 = f"""
   INSERT INTO devdw.monthly_customer_summary
    (SELECT date_trunc('month', summary_date::date) start_of_the_month_date,
       dw_customer_id,
       SUM(order_count),
       SUM(order_apd),
       (CASE WHEN SUM(order_apd) > 0 THEN 1 ELSE 0 END) order_apm,
       SUM(ordered_amt),
       SUM(order_cost_amount),
       SUM(order_mrp_amount),
       SUM(cancelled_order_count),
       SUM(cancelled_order_amount),
       SUM(cancelled_order_apd),
       (CASE WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0 END)
         cancelled_order_apm,
       SUM(shipped_order_count),
       SUM(shipped_order_amount),
       SUM(shipped_order_apd),
       (CASE WHEN SUM(shipped_order_apd) > 0 THEN 1 ELSE 0 END)
         shipped_order_apm,
       SUM(payment_apd),
       (CASE WHEN SUM(payment_apd) > 0 THEN 1 ELSE 0 END) payment_apm,
       SUM(payment_amount),
       SUM(new_customer_apd),
       (CASE WHEN SUM(new_customer_apd) > 0 THEN 1 ELSE 0 END)
         new_customer_apm,
       SUM(new_customer_paid_apd),
       (CASE WHEN SUM(new_customer_paid_apd) > 0 THEN 1 ELSE 0 END)
         new_customer_paid_apm,
        current_timestamp dw_create_timestamp,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM devdw.daily_customer_summary
    WHERE date_trunc('month', summary_date::date)>=
      date_trunc('month', '{ETL_BATCH_DATE}'::date)
    GROUP BY date_trunc('month', summary_date::date),
         dw_customer_id);    """
    cur.execute(sql1)
    cur.execute(sql2)
    conn.commit()
    print("Monthly Customer Summary data loaded")
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
load_monthly_customer_smy(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
