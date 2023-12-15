import os
import snowflake.connector
import logging

for logger_name in ['snowflake','botocore']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        ch = logging.FileHandler('/logs/python_connector.log')
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

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello, World!"

if __name__ == '__main__':
    app.run(port=8080)

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
    import time
    
    print("set 1")
    con = get_con()
    cur = con.cursor()
    result1 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between 0 and 2000000")
    result1 = result1.fetchall()
    count = 1
    for row in result:
        if count < 5:
            print(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    
    print("set 2")
    con = get_con()
    cur = con.cursor()
    result2 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between 2000001 and 4000000")
    result2 = result2.fetchall()
    count = 1
    for row in result:
        if count < 5:
            print(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    
    print("set 3")
    con = get_con()
    cur = con.cursor()
    result3 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between 4000001 and 6000000")
    result3 = result3.fetchall()
    count = 1
    for row in result:
        if count < 5:
            print(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    
    
    time.sleep(os.environ.get("SF_TIMEOUT", "900"))
    print("set 4")
    con = get_con()
    cur = con.cursor()
    result4 = cur.execute(f"select * from table(result_scan('{query_id}')) where rownum between 6000001 and 8000000")
    result4 = result4.fetchall()
    count = 1
    for row in result:
        if count < 5:
            print(row)
    
        count = count + 1
    
    cur.close()
    con.close()
    
    print("DONE")


