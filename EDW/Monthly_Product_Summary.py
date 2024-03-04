import psycopg2

ETL_BATCH_NO = 1001
ETL_BATCH_DATE = '2001-01-01'


def load_monthly_product_smy(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   DELETE FROM devdw.monthly_product_summary
   WHERE start_of_the_month_date>= date_format('{ETL_BATCH_DATE}','%Y-%m-01');
   """
    sql2 = f"""
   INSERT INTO devdw.monthly_product_summary
    (SELECT date_format(summary_date,'%Y-%m-01') start_of_the_date,
       dw_product_id,
       SUM(customer_apd) customer_apd,
       (CASE WHEN SUM(customer_apd) > 0 THEN 1 ELSE 0 END) customer_apm,
       SUM(product_cost_amount) product_cost_amount,
       SUM(product_mrp_amount) product_mrp_amount,
       SUM(cancelled_product_qty) cancelled_product_qty,
       SUM(cancelled_cost_amount) cancelled_cost_amount,
       SUM(cancelled_mrp_amount) cancelled_mrp_amount,
       SUM(cancelled_order_apd) cancelled_order_apd,
       (CASE WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0 END)
       cancelled_order_apm,
       current_timestamp dw_create_timestamp,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM devdw.daily_product_summary
    WHERE date_format(summary_date,'%Y-%m-01')>=
      date_format('${ETL_BATCH_DATE}','%Y-%m-01')
    GROUP BY date_format(summary_date,'%Y-%m-01'),
         dw_product_id);
    """
    cur.execute(sql1)
    cur.execute(sql2)
    conn.commit()
    print("Monthly Product Summary data loaded")
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
load_monthly_product_smy(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
