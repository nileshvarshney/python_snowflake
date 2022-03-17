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

# create table if not exists
CR_TABLE_EMP = """
    CREATE TABLE IF NOT EXISTS emp
    (
        EMPLOYEE_ID     integer,
        FIRST_NAME      string,
        LAST_NAME       string,
        EMAIL           string,
        PHONE_NUMBER    string,
        HIRE_DATE       date,
        JOB_ID          string,
        SALARY          double,
        COMMISSION_PCT  double,
        MANAGER_ID      integer,
        DEPARTMENT_ID   integer
    )"""

CR_TABLE_DEPT = """
    CREATE TABLE IF NOT EXISTS dept
    (
        DEPARTMENT_ID   INTEGER,
        DEPARTMENT_NAME STRING,
        MANAGER_ID      INTEGER,
        LOCATION_ID     INTEGER
    )
"""

CR_TABLE_LOCATION = """
    CREATE TABLE IF NOT EXISTS location
    (
        LOCATION_ID     INTEGER,
        STREET_ADDRESS  STRING,
        POSTAL_CODE     STRING,
        CITY            STRING,
        STATE_PROVINCE  STRING,
        COUNTRY_ID      STRING
    )
"""

MOD_EMP_RECS = """
    UPDATE emp e
    SET 
        e.FIRST_NAME = t.FIRST_NAME ,
        e.LAST_NAME = t.LAST_NAME ,
        e.EMAIL = t.EMAIL ,
        e.PHONE_NUMBER = t.PHONE_NUMBER ,
        e.HIRE_DATE = t.HIRE_DATE ,
        e.JOB_ID = t.JOB_ID ,
        e.SALARY = t.SALARY ,
        e.COMMISSION_PCT = t.COMMISSION_PCT ,
        e.MANAGER_ID = t.MANAGER_ID ,
        e.DEPARTMENT_ID = t.DEPARTMENT_ID
  FROM @%emp t 
  where t.employee_id = e.employee_id
  and (
        t.FIRST_NAME != e.FIRST_NAME OR
        t.LAST_NAME != e.LAST_NAME OR
        t.EMAIL != e.EMAIL OR
        t.PHONE_NUMBER != e.PHONE_NUMBER OR
        t.HIRE_DATE != e.HIRE_DATE OR
        t.JOB_ID != e.FIRST_NAME OR
        t.SALARY != e.SALARY OR
        t.COMMISSION_PCT != e.COMMISSION_PCT OR
        t.MANAGER_ID != e.MANAGER_ID OR
        t.DEPARTMENT_ID != e.DEPARTMENT_ID )
"""

INS_EMP_RECS ="""
    INSERT INTO emp
    SELECT * FROM @%emp t 
    WHERE NOT EXISTS ( 
        SELECT 1 FROM emp e 
        WHERE t.employee_id = e.employee_id)

"""

MOD_DEPT_RECS = """
    UPDATE dept d
    SET 
        d.DEPARTMENT_NAME = t.DEPARTMENT_NAME ,
        d.MANAGER_ID = t.MANAGER_ID ,
        d.LOCATION_ID = t.LOCATION_ID 
  FROM @%dept t 
  where t.DEPARTMENT_ID = d.DEPARTMENT_ID
  and (
        t.DEPARTMENT_NAME != d.DEPARTMENT_NAME OR
        t.MANAGER_ID != d.MANAGER_ID OR
        t.LOCATION_ID != d.LOCATION_ID)
"""

INS_DEPT_RECS ="""
    INSERT INTO dept
    SELECT department_id, department_name, 
    case when manager_id = 'NULL' then NULL else manager_id end manager_id, location_id 
    FROM @%dept t 
    WHERE NOT EXISTS ( 
        SELECT 1 FROM dept d 
        WHERE t.department_id = d.department_id)

"""

MOD_LOC_RECS = """
    UPDATE location l
    SET 
        l.STREET_ADDRESS = t.STREET_ADDRESS ,
        l.POSTAL_CODE = t.POSTAL_CODE ,
        l.CITY = t.CITY ,
        l.STATE_PROVINCE = t.STATE_PROVINCE,
        l.COUNTRY_ID = t.COUNTRY_ID 
  FROM @%location t 
  where t.location_id = l.location_id
  and (
        t.STREET_ADDRESS != l.STREET_ADDRESS OR
        t.POSTAL_CODE != l.POSTAL_CODE OR
        t.CITY != l.CITY OR
        t.STATE_PROVINCE != l.STATE_PROVINCE OR
        t.COUNTRY_ID != l.COUNTRY_ID)
"""

INS_LOC_RECS ="""
    INSERT INTO location
    SELECT * FROM @%location t 
    WHERE NOT EXISTS ( 
        SELECT 1 FROM location l 
        WHERE t.location_id = l.location_id)

"""

DML_STMTS = [MOD_EMP_RECS, INS_EMP_RECS,  INS_DEPT_RECS, MOD_LOC_RECS, INS_LOC_RECS]
cs = ctx.cursor()

def create_tables(cr_tab_sql):
    """Create tables based on provided SQL scripts

    Args:
        cursor (snowflake cursor): Snow flake cursor
        cr_tab_sql (string): SQL string to create table
    """

    try:
        cs.execute(cr_tab_sql)
    except conn.Error as err:
        print(f'Erorr while creating table emp table: {err}')
        cs.close()
        ctx.close()


def remove_table_stages_data(table_name):
    """ Remove previously uploaded stage data

    Args:
        table_name (string): remove staged data from
        provided database 
    """
    remove_stage = f'REMOVE  @%{table_name}'
    print(remove_stage)
    try:
        cs.execute(remove_stage)
    except conn.Error as err:
        print(f'Erorr while removing stage data: {err}')
        cs.close()
        ctx.close()


def populate_stage(file_location, table_name):
    """Poplutae table stage from provided data

    Args:
        file_location (string): file path
        table_name (string): database table
    """
    populate_stage_stmt = f"PUT file://{file_location} @%{table_name}"
    print(populate_stage_stmt)
    try:
        cs.execute(populate_stage_stmt)
        #cs.execute("PUT file:///tmp/employee*.csv @%emp")
        #cs.execute("COPY INTO emp")
    except conn.Error as err:
        print(f'Erorr while populating {table_name} stage : {err}')
        print('connection closed')
        cs.close()
        ctx.close()


def execute_dml(dml):
    """Execute any DML statement

    Args:
        file_location (string): File path
        table_name (string): database table
    """
    print(f'executing :{dml}')
    try:
        cs.execute(dml)
        ctx.commit()
    except conn.Error as err:
        print(f'Erorr while executing : {dml} : {err}')
        print('connection closed')
        cs.close()
        ctx.close()


if __name__ == "__main__":
    tables = ['emp', 'dept', 'location']
    cr_table_stmts = [CR_TABLE_EMP, CR_TABLE_DEPT, CR_TABLE_LOCATION]
    data_locations = ['/tmp/employee*.csv', '/tmp/department*.csv', '/tmp/location*.csv']

    for cr_table in cr_table_stmts:
        create_tables(cr_table)

    # delete data from table stage prior to load
    for clean_stage in tables:
        remove_table_stages_data(clean_stage)

    # populate stage
    table_filelocations = zip(tables, data_locations)
    for table, data_location in table_filelocations:
        populate_stage(data_location, table)
    
    # dml execution
    for stmt in DML_STMTS:
        if not ctx.is_closed():
            execute_dml(stmt)
        else:
            print('Connection closed further execution escapped...Please check log')

    if not ctx.is_closed():
        ctx.close()
