import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_orderdetails(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   insert into devdw.orderdetails
(
 dw_order_id           ,
   dw_product_id        ,
   src_orderNumber      ,
   src_productCode      ,
   quantityOrdered      ,
   priceEach            ,
   orderLineNumber       ,
   src_create_timestamp  ,
   src_update_timestamp ,
   etl_batch_no,
   etl_batch_date
)
SELECT
  o.dw_order_id
, p.dw_product_id
, od.ORDERNUMBER
, od.PRODUCTCODE
, od.QUANTITYORDERED
, od.PRICEEACH
, od.ORDERLINENUMBER
, od.create_timestamp
, od.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM devstage.OrderDetails od left join devdw.orderdetails
 od1 on od.orderNumber=od1.src_orderNumber
 and od.productCode=od1.src_productCode
inner join devdw.orders o on o.src_ordernumber=od.ordernumber
inner join devdw.products p on od.productcode=p.src_productcode
where od1.src_orderNumber is null and od1.src_productCode is null;
"""
    sql2 = f"""
  update devdw.orderdetails a
set
    src_orderNumber          =b.ordernumber,
    src_productCode        =b.productcode,
    quantityOrdered       =b.quantityordered,
    priceEach             =b.priceeach,
    orderLineNumber       =b.orderlinenumber,
    src_update_timestamp  =b.update_timestamp,
    dw_update_timestamp   =current_timestamp,
    etl_batch_no          ='{ETL_BATCH_NO}' ,
    etl_batch_date        ='{ETL_BATCH_DATE}'
   from devstage.OrderDetails b
where a.src_ordernumber=b.ordernumber and a.src_productcode=b.productcode;

    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" OrderDetails data loaded")
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
load_orderdetails(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
