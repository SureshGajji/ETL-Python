import psycopg2

ETL_BATCH_NO = 1001
ETL_BATCH_DATE = '2001-01-01'


def load_daily_product_summary(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   insert into devdw.daily_product_summary
    (select summary_date,
    dw_product_id,
    max(customer_apd) customer_apd,
    max(product_cost_amount) product_cost_amount,
    max(product_mrp_amount) product_mrp_amount,
    max(cancelled_product_qty) cancelled_product_qty,
    max(cancelled_cost_amount) cancelled_cost_amount,
    max(cancelled_mrp_amount) cancelled_mrp_amount,
    max(cancelled_order_apd)cancelled_order_apd,
    CURRENT_TIMESTAMP dw_create_timestamp,
    '{ETL_BATCH_NO}' etl_batch_no,
    '{ETL_BATCH_DATE}' etl_batch_date
    from (SELECT CAST(o.orderDate AS DATE) summary_date,
       p.dw_product_id,
       count(distinct o.src_customerNumber) customer_apd,
       sum(od.priceeach*od.quantityordered) product_cost_amount,
       sum(p.msrp*od.quantityordered) product_mrp_amount,
       0 cancelled_product_qty,
       0 cancelled_cost_amount,
       0 cancelled_mrp_amount,
       0 cancelled_order_apd
    FROM devdw.products p
     INNER JOIN devdw.orderdetails
     od ON p.dw_product_id = od.dw_product_id
     JOIN devdw.orders o ON od.dw_order_id = o.dw_order_id
     WHERE CAST(o.orderDate AS DATE) >='{ETL_BATCH_DATE}'
    GROUP BY CAST(o.orderDate AS DATE),
         p.dw_product_id
    union all
    SELECT CAST(o.cancelledDate AS DATE) summary_date,
       p.dw_product_id,
       0 customer_apd,
       0 product_cost_amount,
       0 product_mrp_amount,
       count(distinct p.src_productcode) cancelled_product_qty,
       sum(od.priceeach*od.quantityordered) cancelled_cost_amount,
       sum(p.msrp*od.quantityordered) cancelled_mrp_amount,
       1 cancelled_order_apd
    FROM devdw.products p
    INNER JOIN devdw.orderdetails od
    ON p.dw_product_id = od.dw_product_id
    JOIN devdw.orders o ON od.dw_order_id = o.dw_order_id
     WHERE CAST(o.cancelledDate AS DATE) >='{ETL_BATCH_DATE}'
     and o.status='Cancelled'
    GROUP BY CAST(o.cancelledDate AS DATE),
    p.dw_product_id)a
    group by summary_date,dw_product_id);
"""
    cur.execute(sql1)
    conn.commit()
    print("Daily Product Summary data loaded")
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
load_daily_product_summary(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
