import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_customers(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.customers
   ( src_customerNumber     ,
     customerName           ,
     contactLastName        ,
     contactFirstName       ,
     phone                  ,
     addressLine1           ,
     addressLine2           ,
     city                   ,
     state                  ,
     postalCode             ,
     country                ,
     salesRepEmployeeNumber ,
     dw_salesrep_id         ,
     creditLimit            ,
     src_create_timestamp   ,
     src_update_timestamp   ,
     etl_batch_no           ,
     etl_batch_date
    )
   SELECT
    c.CUSTOMERNUMBER
  , c.CUSTOMERNAME
  , c.CONTACTLASTNAME
  , c.CONTACTFIRSTNAME
  , c.PHONE
  , c.ADDRESSLINE1
  , c.ADDRESSLINE2
  , c.CITY
  , c.STATE
  , c.POSTALCODE
  , c.COUNTRY
  , c.SALESREPEMPLOYEENUMBER
  , e.dw_employee_id
  , c.CREDITLIMIT
  , c.create_timestamp
  , c.update_timestamp
  , '{ETL_BATCH_NO}'
  , '{ETL_BATCH_DATE}'
  FROM devstage.Customers c left join devdw.customers c1
  on c.customerNumber=c1.src_customerNumber
  left join devdw.employees e
  on c.salesrepemployeenumber=e.employeenumber
  where c1.src_customerNumber is null;
  """
    sql2 = f"""
  UPDATE devdw.customers a
  SET src_customerNumber   =b.CUSTOMERNUMBER,
      customerName           =b.CUSTOMERNAME,
      contactLastName        =b.CONTACTLASTNAME,
      contactFirstName       =b.CONTACTFIRSTNAME,
      phone                  =b.PHONE,
      addressLine1           =b.ADDRESSLINE1,
      addressLine2           =b.ADDRESSLINE2,
      city                   =b.CITY,
      state                  =b.STATE,
      postalCode             =b.POSTALCODE,
      country                =b.COUNTRY,
      salesRepEmployeeNumber =b.SALESREPEMPLOYEENUMBER,
      creditLimit            =b.CREDITLIMIT,
      src_update_timestamp   =b.update_timestamp,
      dw_update_timestamp    =CURRENT_TIMESTAMP,
      etl_batch_no           ='{ETL_BATCH_NO}',
      etl_batch_date         ='{ETL_BATCH_DATE}'
      from devstage.Customers b
      where a.src_customerNumber=b.CUSTOMERNUMBER;
    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Customers data loaded")
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
load_customers(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
