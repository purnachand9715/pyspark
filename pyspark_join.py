from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark join example').getOrCreate()

empData = [(1, 'Alice', 'Smith', 1, 50000, '2020-01-15'),
(2, 'Bob', 'Johnson', 1, 60000, '2018-03-22'),
(3, 'Charlie', 'Williams', 2, 70000, '2019-07-30'),
(4, 'David', 'Brown', 2, 80000, '2017-11-11'),
(5, 'Eve', 'Davis', 3, 90000, '2021-02-25'),
(6, 'Frank', 'Miller', 3, 55000, '2020-09-10'),
(7, 'Grace', 'Wilson', 2, 75000, '2016-04-05'),
(8, 'Henry', 'Moore', 1, 65000, '2022-06-17') ]

print(type(empData))

empColumns = ["EmployeeID","FirstName","LastName","DepartmentID",
       "Salary","DateHired"]

deptData= [ (1, 'HR'),
(2, 'Engineering'),
(3, 'Sales')
]
print(type(deptData))

deptColumns = ["DepartmentID", "DepartmentName"]

df_emp = spark.createDataFrame(data=empData, schema=empColumns)

df_emp.printSchema()
df_emp.show(truncate=False)

df_dept = spark.createDataFrame(data=deptData, schema=deptColumns)
df_dept.printSchema()

df_dept.show(truncate=False)

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "inner").show()

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "left").show()

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "right").show()

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "full").show()

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "semi").show()

df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "anti").show()


df_emp.createOrReplaceTempView("emp")

df_dept.createOrReplaceTempView("dept")

spark.sql("select * from emp e inner join dept d ON e.DepartmentID == d.DepartmentID").show()

spark.sql("select * from emp e left join dept d ON e.DepartmentID == d.DepartmentID").show()

spark.sql("select * from emp e right join dept d ON e.DepartmentID == d.DepartmentID").show()

spark.sql("select * from emp e full join dept d ON e.DepartmentID == d.DepartmentID").show()

spark.sql("select * from emp e semi join dept d ON e.DepartmentID == d.DepartmentID").show()

spark.sql("select * from emp e anti join dept d ON e.DepartmentID == d.DepartmentID").show()