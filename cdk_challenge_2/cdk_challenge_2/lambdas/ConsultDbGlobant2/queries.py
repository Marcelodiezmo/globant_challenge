EMPLOYEES_FIRED='''SELECT
  departments.id AS department_id,
  departments.department AS department_name,
  COUNT(hired_employees.id) AS num_hired_employees
FROM
  Globant.departments
  JOIN Globant.hired_employees ON (departments.id = hired_employees.department_id)
WHERE
  YEAR(hired_employees.datetime) = 2021
GROUP BY
  departments.id,
  departments.department
HAVING
  COUNT(hired_employees.id) > (SELECT AVG(num_hired_employees) FROM (SELECT COUNT(id) AS num_hired_employees FROM Globant.hired_employees WHERE YEAR(datetime) = 2021 GROUP BY department_id) AS subquery)
ORDER BY
  num_hired_employees DESC;

'''