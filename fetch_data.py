""" populate data """
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
    warehouse = 'COMPUTE_WH',
    database = 'TEST_DB',
    schema = "EMPLOYEE"
)
SQL_STMT = "SELECT * FROM EMP WHERE SALARY > 10000 ORDER BY SALARY DESC"
try:
    cs = ctx.cursor()
    cs.execute(SQL_STMT)
    print(f'SQL ID ==> {cs.sfqid}')
    print(f'message ==> {cs.messages}')
    print(f'sqlstate ==> {cs.sqlstate}')
    print("Fetch top 3 records")
    records = cs.fetchmany(3)
    print(records)
except conn.DatabaseError as err:
    print(f'Error while executing select statement {err}')
    cs.close()
    ctx.close()
finally:
    if not cs.is_closed():
        cs.close()
    if not ctx.is_closed():
        ctx.close()
    