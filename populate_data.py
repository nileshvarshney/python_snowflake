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
    database = 'TEST_DB'
)

CR_TABLE = """
    CREATE OR REPLACE TABLE 
    employee.emps2(id integer, ename string, manager_id int, 
    dept_no integer, salary float, comm float, hire_date date);
"""

INS_STMT = """
            INSERT INTO employee.emps2
            (id, ename, manager_id, dept_no, salary, comm, hire_date)
             VALUES (1, 'King', 15, 10, 1200.0, 125.0, '2012-02-18')
        """

cs = ctx.cursor()
# create table if not exists
try:
    cs.execute(CR_TABLE)
except conn.Error as err:
    print(f'Erorr while creating table : {err}')
    cs.close()
    ctx.close()

# populate table with sample records
try:
    cs.execute(INS_STMT)
    ctx.commit()
except conn.Error as err:
    print(f'Erorr while populating data : {err}')
    cs.close()
    ctx.close()

# selecting data
try:
    cs.execute('select * from employee.emps2')
    dataset = cs.fetchall()
    print(type(dataset))
    print(dataset)
except conn.Error as err:
    print(f'Erorr while fetching data : {err}')
finally:
    cs.close()
    ctx.close()
