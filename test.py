import os
import snowflake.connector
import logging
import sys
import time

for logger_name in ['snowflake','botocore']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
#        ch = logging.FileHandler('/logs/python_connector.log')
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
        logger.addHandler(ch)

def get_con():
    print("getting connection")
    con = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role='ACCOUNTADMIN',
        warehouse='CC_WAREHOUSE',
        session_parameters={
            'QUERY_TAG': 'chrispythontest',
        }
    )
    print("returning connection")
    return con

def do_request(query_id, range_start, range_end, count):
    print(f"set {count}")
    con = get_con()
    cur = con.cursor()
    result1 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between {range_start} and {range_end}")
    result1 = result1.fetchall()
    count = 1
    for row in result:
        if count < 5:
            print(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    

if __name__ == '__main__':
    print("LETS GET STARTED")
    con = get_con()
    cur = con.cursor()
    print("Running first query")
    result = cur.execute("select c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, row_number() over(order by c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) as ROWNUM from snowflake_sample_data.tpch_sf100.customer")
    print("First query done")
    query_id = cur.sfqid
    print("Query ID:", query_id)
    
    cur.close()
    con.close()
    
    max_rows = 17950502
    count = 1
    range_total = 2000000
    total_rows = 0
    range_start = 0
    range_end = range_start + range_total
    
    while total_rows <= max_rows:
        if count == 4:
            time.sleep(int(os.environ.get("SF_TIMEOUT", "900")))

        do_request(query_id, range_start, range_end, count)
        count = count + 1
        range_start = range_end + 1
        range_end = range_end + range_total
        if range_end > max_rows:
            range_end = max_rows
    
    print("DONE")

