import os
import snowflake.connector
import logging
import sys
import time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

for logger_name in ['snowflake','botocore']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
#        ch = logging.FileHandler('/logs/python_connector.log')
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
        logger.addHandler(ch)

def get_con():
    logger.info("getting connection")
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
    logger.info("returning connection")
    return con

def do_request(query_id, range_start, range_end, count):
    logger.info(f"set {count}")
    con = get_con()
    cur = con.cursor()
    result1 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between {range_start} and {range_end}")
    result1 = result1.fetchall()
    count = 1
    for row in result:
        if count < 5:
            logger.info(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    

if __name__ == '__main__':
    logger.info("LETS GET STARTED")
    con = get_con()
    cur = con.cursor()
    logger.info("Running first query")
    result = cur.execute("select c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, row_number() over(order by c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) as ROWNUM from snowflake_sample_data.tpch_sf100.customer")
    logger.info("First query done")
    query_id = cur.sfqid
    logger.info(f"Query ID: {str(query_id)}")
    
    cur.close()
    con.close()
    
    max_rows = 17950502
    count = 1
    range_total = 2000000
    range_start = 0
    range_end = range_start + range_total
    
    while range_start <= max_rows:
        if count == 4:
            sleep_time = os.environ.get("SF_TIMEOUT", "900")
            logger.info(f"Sleeping {sleep_time} seconds")
            time.sleep(int(sleep_time))

        do_request(query_id, range_start, range_end, count)
        count = count + 1
        range_start = range_end + 1
        range_end = range_end + range_total
        if range_end > max_rows:
            range_end = max_rows
    
    logger.info("DONE")

