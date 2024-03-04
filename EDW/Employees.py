import psycopg2
import sys

ETL_BATCH_NO = sys.argv[1]
ETL_BATCH_DATE = sys.argv[2]


def load_employees(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    sql1 = f"""
   INSERT INTO devdw.employees
   (employeeNumber         ,
    lastName               ,
    firstName              ,
    extension              ,
    email                  ,
    officeCode             ,
    reportsTo              ,
    jobTitle               ,
    dw_office_id           ,
    src_create_timestamp   ,
    src_update_timestamp   ,
    etl_batch_no           ,
    etl_batch_date
   )
  SELECT
   e.EMPLOYEENUMBER
 , e.LASTNAME
 , e.FIRSTNAME
 , e.EXTENSION
 , e.EMAIL
 , e.OFFICECODE
 , e.REPORTSTO
 , e.JOBTITLE
 , o.dw_office_id
 , e.create_timestamp
 , e.update_timestamp
 ,'{ETL_BATCH_NO}'
 ,'{ETL_BATCH_DATE}'
 FROM devstage.Employees e left join devdw.employees e1
 on e.employeeNumber=e1.employeeNumber
 inner join devdw.offices o on e.officecode=o.officecode
 where e1.employeeNumber is null;
    """
    sql2 = """
  UPDATE devdw.employees a
  SET dw_reporting_employee_id=b.dw_employee_id
  from devdw.employees b
  where a.reportsTo=b.employeeNumber;
    """
    sql3 = f"""
  UPDATE devdw.employees a
  SET
  employeeNumber            =b.EMPLOYEENUMBER,
  lastName               =b.LASTNAME,
  firstName              =b.FIRSTNAME,
  extension              =b.EXTENSION,
  email                  =b.EMAIL,
  officeCode             =b.OFFICECODE,
  reportsTo              =b.REPORTSTO,
  jobTitle               =b.JOBTITLE,
  src_update_timestamp   =b.update_timestamp,
  dw_update_timestamp    =CURRENT_TIMESTAMP,
  etl_batch_no           ='{ETL_BATCH_NO}',
  etl_batch_date         ='{ETL_BATCH_DATE}'
  from devstage.Employees b
  where a.employeeNumber =b.employeeNumber;
    """
    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)
    print(" Employees data loaded")
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
load_employees(redshift_params, ETL_BATCH_NO, ETL_BATCH_DATE)
