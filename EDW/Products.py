import psycopg2

ETL_BATCH_NO = 1001
ETL_BATCH_DATE = '2001-01-01'


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
   insert into devdw.products
  (
     src_productCode       ,
     productName         ,
     productLine           ,
     productScale          ,
     productVendor         ,
     quantityInStock       ,
     buyPrice              ,
     MSRP                  ,
     dw_product_line_id    ,
     src_create_timestamp  ,
     src_update_timestamp  ,
     etl_batch_no,
     etl_batch_date
  )
SELECT
  p.PRODUCTCODE
, p.PRODUCTNAME
, p.PRODUCTLINE
, p.PRODUCTSCALE
, p.PRODUCTVENDOR
, p.QUANTITYINSTOCK
, p.BUYPRICE
, p.MSRP
, pl.dw_product_line_id
, p.create_timestamp
, p.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM devstage.Products p left join devdw.products p1
on p.productCode=p1.src_productCode
inner join devdw.productlines pl on p.productline=pl.productline
where p1.src_productCode is null;
"""
    sql2 = f"""
  update devdw.products a
  set
    productName         =b.productname,
    productLine           =b.productline,
    productScale          =b.productscale,
    productVendor         =b.productvendor,
    quantityInStock       =b.quantityinstock,
    buyPrice              =b.buyprice,
    MSRP                  =b.msrp,
    src_update_timestamp  =b.update_timestamp,
    dw_update_timestamp   =current_timestamp,
    etl_batch_no          ='{ETL_BATCH_NO}',
    etl_batch_date        ='{ETL_BATCH_DATE}'
    from devstage.Products b
  where a.src_productcode=b.productcode ;
    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Products data loaded")
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
