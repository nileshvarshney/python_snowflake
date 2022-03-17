
"""
    Clean emoployee data in required format
"""
import pandas as pd


emp_raw_df = pd.read_csv('./employee_raw.csv', infer_datetime_format=True)
emp_raw_df['HIRE_DATE'] = pd.to_datetime(emp_raw_df['HIRE_DATE'])
emp_raw_df[
    ['SALARY','COMMISSION_PCT','MANAGER_ID','DEPARTMENT_ID']
] = emp_raw_df[
    ['SALARY','COMMISSION_PCT','MANAGER_ID','DEPARTMENT_ID']
    ].fillna(0)

emp_raw_df.to_csv("/tmp/employee.csv", index=False, header=False)
