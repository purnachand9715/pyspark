from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, row_number

spark = SparkSession.builder.appName("pyspark window function example ").getOrCreate()

empData = [(1, 'Alice', 'Smith', 1, 50000, '2020-01-15'),
(2, 'Bob', 'Johnson', 1, 60000, '2018-03-22'),
(3, 'Charlie', 'Williams', 2, 70000, '2019-07-30'),
(4, 'David', 'Brown', 2, 80000, '2017-11-11'),
(5, 'Eve', 'Davis', 3, 90000, '2021-02-25'),
(6, 'Frank', 'Miller', 3, 55000, '2020-09-10'),
(7, 'Grace', 'Wilson', 2, 75000, '2016-04-05'),
(8, 'Henry', 'Moore', 1, 65000, '2022-06-17') ]

empColumns = ["EmployeeID","FirstName","LastName","DepartmentID",
       "Salary","DateHired"]

deptData= [ (1, 'HR'),
(2, 'Engineering'),
(3, 'Sales')
]

deptColumns = ["DepartmentID", "DepartmentName"]

df_emp = spark.createDataFrame(data=empData, schema=empColumns)

df_emp.printSchema()
#df_emp.show(truncate=False)

df_dept = spark.createDataFrame(data=deptData, schema=deptColumns)

df_dept.printSchema()

#df_dept.show()

#join emp & dept columns

df_join = df_emp.join(df_dept, df_emp.DepartmentID == df_dept.DepartmentID, "inner")

windowspec = Window.partitionBy("DepartmentName").orderBy(desc("Salary"))

# find the top 2 highest paid employees in each dept using window function

df_join.withColumn("rnk", row_number().over(windowspec)).filter("rnk <=2 ").show()


#Using sql

df_emp.createOrReplaceTempView("emp")

df_dept.createOrReplaceTempView("dept")

spark.sql(" with cte as (select e.EmployeeID,e.FirstName,e.LastName,e.DepartmentID,e.Salary,e.DateHired,d.DepartmentName,row_number() over(partition by DepartmentName order by Salary desc) as rnk from emp e join dept d on e.DepartmentID == d.DepartmentID) select * from cte where rnk <=2 ").show()