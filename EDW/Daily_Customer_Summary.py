import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_daily_customer_summary(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   insert into devdw.daily_customer_summary(
    SELECT summary_date,
       dw_customer_id,
       MAX(order_count) order_count,
       MAX(customer_unique_count) customer_unique_count,
       MAX(ordered_amt) ordered_amt,
       MAX(order_cost_amount) order_cost_amount,
       MAX(order_mrp_amount) order_mrp_amount,
       MAX(cancelled_order_count) cancelled_order_count,
       MAX(cancelled_order_amount) cancelled_order_amount,
       MAX(cancelled_order_apd) cancelled_order_apd,
       MAX(shipped_order_count) shipped_order_count,
       MAX(shipped_order_amount) shipped_order_amount,
       MAX(shipped_order_apd) shipped_order_apd,
       MAX(payment_apd) payment_apd,
       MAX(payment_amount) payment_amount,
       MAX(new_customer_apd) new_customer_apd,
       MAX(new_customer_paid_apd) new_customer_paid_apd,
       CURRENT_TIMESTAMP dw_create_timestamp,
       '{ETL_BATCH_NO}' etl_batch_no,
       '{ETL_BATCH_DATE}' etl_batch_date
    FROM (SELECT CAST(o.orderdate AS DATE) summary_date,
             o.dw_customer_id,
             COUNT(DISTINCT o.src_ordernumber) order_count,
             1 customer_unique_count,
             SUM(od.quantityordered*od.priceeach) ordered_amt,
             SUM(p.buyprice*od.quantityordered) order_cost_amount,
             SUM(p.msrp*od.quantityordered) order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
        FROM devdw.orders o
        JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
        JOIN devdw.products p ON od.dw_product_id = p.dw_product_id
        WHERE CAST(o.orderdate AS DATE) >= '{ETL_BATCH_DATE}'
        GROUP BY CAST(o.orderdate AS DATE),
               o.dw_customer_id
      UNION ALL
      SELECT CAST(o.cancelleddate AS DATE) summary_date,
             o.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amt,
             0 order_cost_amount,
             0 order_mrp_amount,
             COUNT(o.src_ordernumber) cancelled_order_count,
             SUM(od.quantityordered*od.priceeach) cancelled_order_amount,
             1 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
      FROM devdw.orders o
        JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
      WHERE CAST(o.cancelleddate AS DATE) >= '{ETL_BATCH_DATE}'
      AND   o.status = 'Cancelled'
      GROUP BY CAST(o.cancelleddate AS DATE),
               o.dw_customer_id
      UNION ALL
      SELECT CAST(o.shippeddate AS DATE) summary_date,
             o.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amt,
             0 order_cost_amount,
             0 order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             COUNT(DISTINCT o.src_ordernumber) shipped_order_count,
             SUM(od.quantityordered*od.priceeach) shipped_order_amount,
             1 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
FROM devdw.orders o
  JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
WHERE CAST(o.shippeddate AS DATE) >= '{ETL_BATCH_DATE}'
AND   o.status = 'Shipped'
GROUP BY CAST(o.shippeddate AS DATE),
         o.dw_customer_id
UNION ALL
SELECT CAST(p.paymentdate AS DATE) summary_date,
       p.dw_customer_id,
       0 order_count,
       0 customer_unique_count,
       0 ordered_amt,
       0 order_cost_amount,
       0 order_mrp_amount,
       0 cancelled_order_count,
       0 cancelled_order_amount,
       0 cancelled_order_apd,
       0 shipped_order_count,
       0 shipped_order_amount,
       0 shipped_order_apd,
       1 payment_apd,
       SUM(p.amount) payment_amount,
       0 new_customer_apd,
       0 new_customer_paid_apd
FROM devdw.payments p
WHERE CAST(p.paymentdate AS DATE) >= '{ETL_BATCH_DATE}'
GROUP BY CAST(p.paymentdate AS DATE),
         p.dw_customer_id
UNION ALL
SELECT CAST(c.src_create_timestamp AS DATE) summary_date,
       c.dw_customer_id,
       0 order_count,
       0 customer_unique_count,
       0 ordered_amt,
       0 order_cost_amount,
       0 order_mrp_amount,
       0 cancelled_order_count,
       0 cancelled_order_amount,
       0 cancelled_order_apd,
       0 shipped_order_count,
       0 shipped_order_amount,
       0 shipped_order_apd,
       0 payment_apd,
       0 payment_amount,
       1 new_customer_apd,
       0 new_customer_paid_apd
FROM devdw.customers c
WHERE CAST(c.src_create_timestamp AS DATE) >= '{ETL_BATCH_DATE}'
GROUP BY CAST(c.src_create_timestamp AS DATE),
         c.dw_customer_id
UNION ALL
SELECT MIN(CAST(o.orderdate AS DATE)) summary_date,
       o.dw_customer_id,
       0 order_count,
       0 customer_unique_count,
       0 ordered_amt,
       0 order_cost_amount,
       0 order_mrp_amount,
       0 cancelled_order_count,
       0 cancelled_order_amount,
       0 cancelled_order_apd,
       0 shipped_order_count,
       0 shipped_order_amount,
       0 shipped_order_apd,
       0 payment_apd,
       0 payment_amount,
       0 new_customer_apd,
       1 new_customer_paid_apd
FROM devdw.orders o
GROUP BY o.dw_customer_id
HAVING summary_date >= '{ETL_BATCH_DATE}') a
GROUP BY summary_date,
         dw_customer_id);
"""
    cur.execute(sql1)
    conn.commit()
    print("Daily Customer Summary data loaded")
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
load_daily_customer_summary(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
