import psycopg2


def copy_data_from_s3_to_redshift(bucket_name, file_path, redshift_params):
    conn = psycopg2.connect(
        dbname=redshift_params['dbname'],
        user=redshift_params['user'],
        password=redshift_params['password'],
        host=redshift_params['host'],
        port=redshift_params['port']
    )
    cur = conn.cursor()
    s3_path = f's3://{bucket_name}/{file_path}'
    sql = f"""
    COPY {redshift_params['schema']}.{redshift_params['table']}
    FROM '{s3_path}'
    IAM_ROLE '{redshift_params['iam_role']}'
    CSV
    IGNOREHEADER 1
    """
    print(" Products data loaded")
    cur.execute(sql)
    conn.commit()
    conn.close()


if __name__ == "__main__":

    bucket_name = 'etlpython'
    file_path = 'Products.csv'
    redshift_params = {
        'dbname': 'dev',
        'user': 'admin',
        'password': 'Surya183',
        'host': ('default-workgroup.278921652245.us-east-1.'
                 'redshift-serverless.amazonaws.com'),
        'port': '5439',
        'schema': 'devstage',
        'table': 'products',
        'iam_role': ('arn:aws:iam::278921652245:role/service-role'
                     '/AmazonRedshift-CommandsAccessRole-20240221T231641')
    }
copy_data_from_s3_to_redshift(bucket_name, file_path, redshift_params)
