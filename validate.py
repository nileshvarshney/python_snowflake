""" Validate snowflake connection"""
import os
import snowflake.connector as conn


USER = os.getenv('s_user')
PASSWORD = os.getenv('s_pass')
ACCOUNT = os.getenv('s_acc')

ctx = conn.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    session_parameters = {
        'QUERY_TAG' : 'Snowflake connection Validation'
    },
    warehouse='COMPUTE_WH',
    database = 'test_db',
    schema = 'test_schema'
)
cs = ctx.cursor()
try:
    cs.execute("ALTER SESSION SET QUERY_TAG = 'connection validate'")
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
except conn.Error as err:
    print(f'Error while conncting :{err}')
finally:
    cs.close()
    ctx.close()
