EMPLOYEES_FIRED='''SELECT 
  departments.department,
  jobs.job,
  COUNT(CASE 
         WHEN MONTH(hired_employees.datetime) BETWEEN 1 AND 3 THEN hired_employees.id 
         ELSE NULL 
       END) AS Q1,
  COUNT(CASE 
         WHEN MONTH(hired_employees.datetime) BETWEEN 4 AND 6 THEN hired_employees.id 
         ELSE NULL 
       END) AS Q2,
  COUNT(CASE 
         WHEN MONTH(hired_employees.datetime) BETWEEN 7 AND 9 THEN hired_employees.id 
         ELSE NULL 
       END) AS Q3,
  COUNT(CASE 
         WHEN MONTH(hired_employees.datetime) BETWEEN 10 AND 12 THEN hired_employees.id 
         ELSE NULL 
       END) AS Q4
FROM 
  Globant.departments
  JOIN Globant.hired_employees ON (departments.id = hired_employees.department_id)
  JOIN Globant.jobs ON (hired_employees.job_id = jobs.id)
WHERE 
  YEAR(hired_employees.datetime) = 2021
GROUP BY 
  departments.department,
  jobs.job
'''