import psycopg2

ETL_BATCH_NO = 1001
ETL_BATCH_DATE = '2001-01-01'


def load_offices(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
    INSERT INTO devdw.offices
  (officeCode,
   city,
   phone,
   addressLine1          ,
   addressLine2          ,
   state                 ,
   country               ,
   postalCode            ,
   territory             ,
   src_create_timestamp  ,
   src_update_timestamp ,
   etl_batch_no,
   etl_batch_date
  )
 SELECT
   A.OFFICECODE
 , A.CITY
 , A.PHONE
 , A.ADDRESSLINE1
 , A.ADDRESSLINE2
 , A.STATE
 , A.COUNTRY
 , A.POSTALCODE
 , A.TERRITORY
 , A.create_timestamp
 , A.update_timestamp
 , '{ETL_BATCH_NO}'
 , '{ETL_BATCH_DATE}'
 FROM devstage.Offices A left join devdw.offices B
  ON A.officeCode=B.officeCode
  where B.officeCode IS NULL;
    """
    sql2 = f"""
  UPDATE devdw.offices a
  set
   officeCode            =b.OFFICECODE,
   city                  =b.CITY,
   phone                 =b.PHONE,
   addressLine1          =b.ADDRESSLINE1,
   addressLine2          =b.ADDRESSLINE2,
   state                 =b.STATE,
   country               =b.COUNTRY,
   postalCode            =b.POSTALCODE,
   territory             =b.TERRITORY,
   src_update_timestamp  =b.update_timestamp,
   dw_update_timestamp   =CURRENT_TIMESTAMP,
   etl_batch_no          ='{ETL_BATCH_NO}',
   etl_batch_date        ='{ETL_BATCH_DATE}'
  from devstage.Offices b
  where a.officeCode =b.OFFICECODE
    """
    cur.execute(sql1)
    cur.execute(sql2)
    print(" Offices data loaded")
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
load_offices(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
