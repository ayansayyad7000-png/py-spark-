# PySpark - Basic to Advanced Practical Guide

A simple, practical, beginner-friendly repository to learn **Apache Spark with Python (PySpark)** from zero to advanced concepts.

This repository focuses on:
- What PySpark is and why we use it
- Installation on Windows and Ubuntu/Linux
- Important terminal commands
- SparkSession and local Spark setup
- DataFrames and Spark SQL
- Reading and writing CSV, JSON and Parquet
- Filtering, sorting, grouping and aggregations
- Null handling and data cleaning
- Joins
- Window functions
- RDD basics
- Performance basics: partitions, cache and explain
- Structured Streaming
- Basic MLlib example
- Running programs with `spark-submit`
- Connecting local code to a Spark cluster
- Git and GitHub workflow

> Current Apache Spark documentation used while preparing this guide: **PySpark 4.2.x**. PySpark 4.2 requires **Python 3.10+** and **Java 17+**.

---

## 1. What is Apache Spark?

Apache Spark is a fast, distributed data processing engine.

It is used when we need to process:
- Large datasets
- Data from many files
- ETL pipelines
- Batch processing
- SQL analytics
- Streaming data
- Machine learning workloads

Instead of processing all data on only one CPU core, Spark can divide work into smaller parts and process them in parallel.

### Simple idea

```text
Large Data
   |
   v
Spark Driver
   |
   +---- Executor 1
   +---- Executor 2
   +---- Executor 3
   |
   v
Result
```

---

## 2. What is PySpark?

**PySpark** is the Python API for Apache Spark.

It allows us to write Spark programs using Python.

Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

data = [("Ayan", 21), ("Ali", 22)]

df = spark.createDataFrame(data, ["name", "age"])

df.show()

spark.stop()
```

---

## 3. Important Spark Components

### Driver

The Driver controls your Spark application.

It:
- Starts the application
- Creates the SparkSession
- Builds the execution plan
- Sends tasks to executors
- Collects results

### SparkSession

SparkSession is the main entry point for working with PySpark DataFrames and Spark SQL.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LearningPySpark").getOrCreate()
```

### Executor

Executors run tasks and process data.

### Cluster Manager

A cluster manager gives computing resources to Spark.

Common options:
- Spark Standalone
- YARN
- Kubernetes

### Partition

Spark divides large data into smaller pieces called **partitions**.

Different partitions can be processed in parallel.

---

# PART A - INSTALLATION

## 4. Check Requirements

PySpark 4.2.x requires:
- Python 3.10 or newer
- Java 17 or newer
- pip

### Windows PowerShell

```powershell
python --version
java -version
pip --version
```

If `python` does not work, also try:

```powershell
py --version
```

---

## 5. Install PySpark on Windows

### Step 1 - Create a project folder

```powershell
mkdir pyspark-practice
cd pyspark-practice
```

### Step 2 - Create a virtual environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, you can run Python using the environment directly or adjust the PowerShell execution policy according to your system policy.

### Step 3 - Upgrade pip

```powershell
python -m pip install --upgrade pip
```

### Step 4 - Install PySpark

```powershell
python -m pip install pyspark
```

For Spark SQL optional dependencies:

```powershell
python -m pip install "pyspark[sql]"
```

### Step 5 - Verify installation

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

### Step 6 - Start the PySpark shell

```powershell
pyspark
```

To exit:

```python
exit()
```

---

## 6. Install PySpark on Ubuntu / Linux / AWS EC2

Update packages:

```bash
sudo apt update
```

Install Python tools:

```bash
sudo apt install -y python3 python3-pip python3-venv
```

Install Java 17:

```bash
sudo apt install -y openjdk-17-jdk
```

Check versions:

```bash
python3 --version
java -version
```

Create project:

```bash
mkdir pyspark-practice
cd pyspark-practice
```

Create virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install PySpark:

```bash
python -m pip install --upgrade pip
python -m pip install pyspark
```

Verify:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Start shell:

```bash
pyspark
```

---

## 7. JAVA_HOME

Normally Java should be discoverable automatically.

Check on Ubuntu:

```bash
readlink -f $(which java)
```

Example Java path may look similar to:

```text
/usr/lib/jvm/java-17-openjdk-amd64/bin/java
```

Then JAVA_HOME is the folder before `/bin/java`.

Temporary example:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Check:

```bash
echo $JAVA_HOME
java -version
```

Do not copy a Java path blindly. Use the path installed on your own machine.

---

# PART B - FIRST PROGRAM

## Practical 1 - Create SparkSession

### Objective

Start a Spark application.

Create `hello_spark.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HelloPySpark")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Application:", spark.sparkContext.appName)

spark.stop()
```

Run:

```bash
python hello_spark.py
```

Or:

```bash
spark-submit hello_spark.py
```

### Meaning of local[*]

```text
local[*]
```

means:

Use the available CPU cores of this computer for local Spark processing.

---

## Practical 2 - Create a DataFrame

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameDemo").getOrCreate()

data = [
    (1, "Ayan", 21, "IT"),
    (2, "Sara", 22, "CSE"),
    (3, "Ali", 20, "IT")
]

columns = ["id", "name", "age", "department"]

df = spark.createDataFrame(data, columns)

df.show()
df.printSchema()

spark.stop()
```

Important commands:

```python
df.show()
df.show(2)
df.printSchema()
df.columns
df.count()
```

---

# PART C - DATAFRAME BASICS

## Practical 3 - Select Columns

```python
df.select("name", "age").show()
```

Select using `col`:

```python
from pyspark.sql.functions import col

df.select(col("name"), col("age")).show()
```

---

## Practical 4 - Filter Rows

Age greater than 20:

```python
df.filter(col("age") > 20).show()
```

Same operation:

```python
df.where(col("age") > 20).show()
```

Multiple conditions:

```python
df.filter(
    (col("age") >= 21) &
    (col("department") == "IT")
).show()
```

OR condition:

```python
df.filter(
    (col("department") == "IT") |
    (col("department") == "CSE")
).show()
```

---

## Practical 5 - Add, Rename and Drop Columns

Add new column:

```python
df2 = df.withColumn("age_after_5_years", col("age") + 5)
df2.show()
```

Rename:

```python
df3 = df.withColumnRenamed("department", "dept")
df3.show()
```

Drop:

```python
df4 = df.drop("age")
df4.show()
```

---

## Practical 6 - Sort and Remove Duplicates

Sort ascending:

```python
df.orderBy(col("age").asc()).show()
```

Sort descending:

```python
df.orderBy(col("age").desc()).show()
```

Remove complete duplicate rows:

```python
df.dropDuplicates().show()
```

Remove duplicates using selected columns:

```python
df.dropDuplicates(["name"]).show()
```

---

# PART D - AGGREGATIONS

## Practical 7 - GroupBy

Count students by department:

```python
df.groupBy("department").count().show()
```

Average age:

```python
from pyspark.sql.functions import avg

df.groupBy("department").agg(
    avg("age").alias("average_age")
).show()
```

Multiple aggregations:

```python
from pyspark.sql.functions import count, min, max, avg

df.groupBy("department").agg(
    count("*").alias("students"),
    min("age").alias("min_age"),
    max("age").alias("max_age"),
    avg("age").alias("avg_age")
).show()
```

---

# PART E - DATA CLEANING

## Practical 8 - Handle Null Values

Show rows containing null:

```python
df.filter(col("name").isNull()).show()
```

Remove rows containing null:

```python
clean_df = df.dropna()
```

Remove rows only when selected column is null:

```python
clean_df = df.dropna(subset=["name"])
```

Fill null values:

```python
filled_df = df.fillna({
    "name": "Unknown",
    "age": 0
})
```

---

# PART F - FILE HANDLING

## Practical 9 - Read CSV

Repository sample:

```text
data/students.csv
```

Read:

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/students.csv")
)

df.show()
df.printSchema()
```

Better for production: define schema instead of always using `inferSchema`.

Example:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("marks", IntegerType(), True)
])

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("data/students.csv")
)
```

---

## Practical 10 - Read JSON

```python
json_df = spark.read.json("data/students.json")
json_df.show()
```

---

## Practical 11 - Read Parquet

```python
parquet_df = spark.read.parquet("output/students_parquet")
parquet_df.show()
```

Parquet is widely used in data engineering because it is column-oriented and usually more efficient than CSV for analytics.

---

## Write Data

Write CSV:

```python
df.write.mode("overwrite").option("header", True).csv("output/csv")
```

Write JSON:

```python
df.write.mode("overwrite").json("output/json")
```

Write Parquet:

```python
df.write.mode("overwrite").parquet("output/parquet")
```

Common write modes:

```text
overwrite
append
ignore
error
```

---

# PART G - SPARK SQL

## Practical 12 - SQL Queries

Register a temporary view:

```python
df.createOrReplaceTempView("students")
```

Run SQL:

```python
result = spark.sql("""
    SELECT department,
           COUNT(*) AS total_students,
           AVG(marks) AS average_marks
    FROM students
    GROUP BY department
    ORDER BY average_marks DESC
""")

result.show()
```

The temporary view exists only for the current Spark session.

---

# PART H - JOINS

## Practical 13 - Join Two DataFrames

Students:

```python
students = spark.createDataFrame([
    (1, "Ayan", 101),
    (2, "Sara", 102),
    (3, "Ali", 101)
], ["student_id", "name", "dept_id"])
```

Departments:

```python
departments = spark.createDataFrame([
    (101, "IT"),
    (102, "CSE"),
    (103, "Mechanical")
], ["dept_id", "department"])
```

Inner join:

```python
students.join(
    departments,
    on="dept_id",
    how="inner"
).show()
```

Left join:

```python
students.join(
    departments,
    on="dept_id",
    how="left"
).show()
```

Common join types:

```text
inner
left
right
full
left_semi
left_anti
cross
```

---

# PART I - STRING, DATE AND CONDITIONAL FUNCTIONS

## Practical 14 - Useful Functions

```python
from pyspark.sql.functions import (
    col,
    upper,
    lower,
    length,
    when,
    current_date,
    year
)

df.select(
    upper(col("name")).alias("NAME"),
    length(col("name")).alias("name_length")
).show()
```

Conditional column:

```python
result = df.withColumn(
    "result",
    when(col("marks") >= 40, "PASS").otherwise("FAIL")
)

result.show()
```

---

# PART J - WINDOW FUNCTIONS

## Practical 15 - Ranking

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, col

window_spec = Window.partitionBy("department").orderBy(col("marks").desc())

ranked = df.withColumn(
    "row_number",
    row_number().over(window_spec)
).withColumn(
    "rank",
    rank().over(window_spec)
).withColumn(
    "dense_rank",
    dense_rank().over(window_spec)
)

ranked.show()
```

Use window functions when you need operations such as:
- Ranking inside groups
- Running totals
- Previous or next row values
- Top N records per category

---

# PART K - RDD BASICS

## Practical 16 - RDD

Modern Spark data engineering usually prefers DataFrames for structured data, but RDDs are useful for understanding Spark fundamentals.

Create RDD:

```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
```

Transformation:

```python
squared = rdd.map(lambda x: x * x)
```

Action:

```python
print(squared.collect())
```

Filter:

```python
even = rdd.filter(lambda x: x % 2 == 0)
print(even.collect())
```

Reduce:

```python
total = rdd.reduce(lambda a, b: a + b)
print(total)
```

---

# PART L - TRANSFORMATIONS AND ACTIONS

## Transformations

Transformations build a new dataset.

Examples:

```text
select
filter
withColumn
groupBy
map
flatMap
```

Spark normally evaluates transformations lazily.

Example:

```python
filtered = df.filter(col("marks") > 70)
```

At this point Spark can build the execution plan without immediately producing the final result.

## Actions

Actions trigger computation.

Examples:

```text
show()
count()
collect()
first()
take()
write
```

Example:

```python
filtered.show()
```

---

# PART M - PERFORMANCE BASICS

## Practical 17 - Check Partitions

```python
print(df.rdd.getNumPartitions())
```

Increase/change partitions:

```python
df2 = df.repartition(4)
```

Reduce partitions:

```python
df3 = df.coalesce(1)
```

Be careful with `coalesce(1)` on large datasets because one partition can become a bottleneck.

---

## Practical 18 - Cache

```python
df.cache()

df.count()
df.groupBy("department").count().show()

df.unpersist()
```

Use cache when the same expensive DataFrame is reused multiple times.

Do not cache every DataFrame.

---

## Practical 19 - Explain Execution Plan

```python
df.filter(col("marks") > 70).explain()
```

More detail:

```python
df.filter(col("marks") > 70).explain("formatted")
```

This is useful for understanding query execution and performance.

---

# PART N - STRUCTURED STREAMING

## Practical 20 - Streaming with Built-in Rate Source

This example does not require Kafka or an external file source.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()

stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()
)

query = (
    stream_df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
```

Stop the program with:

```text
Ctrl + C
```

---

# PART O - MLlib BASIC EXAMPLE

## Practical 21 - Simple Linear Regression

Install ML dependencies if needed:

```bash
python -m pip install "pyspark[ml]"
```

Code:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

data = spark.createDataFrame([
    (1.0, 10.0),
    (2.0, 20.0),
    (3.0, 30.0),
    (4.0, 40.0)
], ["hours", "marks"])

assembler = VectorAssembler(
    inputCols=["hours"],
    outputCol="features"
)

prepared = assembler.transform(data)

model = LinearRegression(
    featuresCol="features",
    labelCol="marks"
).fit(prepared)

predictions = model.transform(prepared)

predictions.select("hours", "marks", "prediction").show()
```

This is only a basic MLlib introduction.

---

# PART P - RUNNING PYSPARK PROGRAMS

## Normal Python Run

```bash
python pyspark_practical.py
```

## spark-submit

```bash
spark-submit pyspark_practical.py
```

## Explicit Local Master

```bash
spark-submit --master "local[*]" pyspark_practical.py
```

## Use only 2 local threads

```bash
spark-submit --master "local[2]" pyspark_practical.py
```

---

# PART Q - HOW CONNECTION WORKS

A common beginner question is:

> How does Python connect to Spark?

When you run this:

```python
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Demo")
    .getOrCreate()
)
```

the structure is approximately:

```text
Your Python Program
       |
       v
    PySpark
       |
       v
 Spark Driver
       |
       v
Local Spark Executors / Worker Threads
       |
       v
 Process Data
```

For a cluster:

```text
Laptop / Client
      |
      v
Spark Driver
      |
      v
Cluster Manager
      |
      +------ Worker 1 / Executors
      +------ Worker 2 / Executors
      +------ Worker 3 / Executors
```

---

# PART R - CONNECT TO A SPARK STANDALONE CLUSTER

Suppose the Spark master URL is:

```text
spark://MASTER_HOST:7077
```

Run:

```bash
spark-submit \
  --master spark://MASTER_HOST:7077 \
  pyspark_practical.py
```

Or configure in Python:

```python
spark = (
    SparkSession.builder
    .appName("ClusterApp")
    .master("spark://MASTER_HOST:7077")
    .getOrCreate()
)
```

Replace `MASTER_HOST` with the real master hostname or IP.

Do not expose a private cluster directly to the public internet without proper network and security controls.

---

# PART S - SPARK CONNECT

Spark Connect uses a client/server model.

Install full Spark Connect dependencies:

```bash
python -m pip install "pyspark[connect]"
```

A remote connection URI can look like:

```text
sc://localhost
```

Conceptually:

```text
Python Client
    |
    v
Spark Connect
    |
    v
Spark Server
    |
    v
Cluster / Data
```

Use Spark Connect when you specifically have a Spark Connect server available.

---

# PART T - USE PYSPARK ON AWS EC2

Basic learning setup:

```text
Windows Laptop
      |
      | SSH
      v
AWS EC2 Ubuntu
      |
      +-- Java
      +-- Python
      +-- PySpark
      |
      v
Run Spark locally on the EC2 machine
```

Connect from Windows PowerShell:

```powershell
cd $HOME\Downloads
ssh -i "your-key.pem" ubuntu@YOUR_EC2_PUBLIC_IP
```

On EC2:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv openjdk-17-jdk
```

Clone this repository:

```bash
git clone https://github.com/ayansayyad7000-png/py-spark-.git
cd py-spark-
```

Create environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Run:

```bash
spark-submit --master "local[*]" pyspark_practical.py
```

This runs Spark on that single EC2 instance. A real multi-node Spark cluster requires separate master/worker or managed cluster configuration.

---

# PART U - GIT + GITHUB WORKFLOW

## Clone repository

```bash
git clone https://github.com/ayansayyad7000-png/py-spark-.git
cd py-spark-
```

## Check status

```bash
git status
```

## Pull latest changes

```bash
git pull origin main
```

## Create a new file

Windows:

```powershell
notepad practice.py
```

Linux:

```bash
nano practice.py
```

## Add files

```bash
git add .
```

## Commit

```bash
git commit -m "Add PySpark practice"
```

## Push

```bash
git push origin main
```

Simple flow:

```text
Laptop
  |
  v
Write PySpark Code
  |
  v
git add .
  |
  v
git commit
  |
  v
git push
  |
  v
GitHub Repository
```

---

# PART V - IMPORTANT COMMAND CHEAT SHEET

## Python and installation

```bash
python --version
java -version
python -m pip --version
python -m pip install pyspark
python -c "import pyspark; print(pyspark.__version__)"
```

## PySpark shell

```bash
pyspark
```

## Submit application

```bash
spark-submit app.py
spark-submit --master "local[*]" app.py
```

## Git

```bash
git status
git add .
git commit -m "message"
git pull origin main
git push origin main
```

---

# PART W - COMMON ERRORS

## Error 1 - java is not recognized / java not found

Check:

```bash
java -version
```

Install Java 17+ and make sure Java is available in PATH.

---

## Error 2 - JAVA_HOME is not set

Find your Java installation and set JAVA_HOME to the Java installation directory, not to the `java.exe` or `bin/java` file itself.

---

## Error 3 - No module named pyspark

Install inside the active Python environment:

```bash
python -m pip install pyspark
```

Then check:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Error 4 - Wrong Python environment

Check Python:

```bash
python --version
```

Check pip:

```bash
python -m pip --version
```

Both should point to the environment you are actually using.

---

## Error 5 - Output creates many part files

Spark writes one output file per output partition.

Example:

```python
df.coalesce(1).write.mode("overwrite").option("header", True).csv("output")
```

This can be acceptable for small learning datasets, but do not force one partition for large production data.

---

# PART X - BEGINNER TO ADVANCED LEARNING ORDER

Follow this order:

1. Python basics
2. Java + PySpark installation
3. SparkSession
4. DataFrame creation
5. `show`, `select`, `filter`
6. `withColumn`
7. Sorting
8. GroupBy and aggregations
9. Null handling
10. CSV / JSON / Parquet
11. Spark SQL
12. Joins
13. Built-in functions
14. Window functions
15. RDD basics
16. Lazy evaluation
17. Partitions
18. Cache
19. Explain plan
20. Structured Streaming
21. MLlib basics
22. `spark-submit`
23. Standalone cluster concepts
24. AWS / cloud execution

---

# PART Y - MINI DATA ENGINEERING PROJECT

Build this after completing the practicals:

```text
students.csv
     |
     v
Read with PySpark
     |
     v
Clean Null Values
     |
     v
Filter Invalid Rows
     |
     v
Create New Columns
     |
     v
Group / Aggregate
     |
     v
Spark SQL
     |
     v
Write Parquet Output
```

Example pipeline:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

spark = (
    SparkSession.builder
    .appName("StudentETL")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/students.csv")
)

clean = (
    df.dropna(subset=["id", "name", "marks"])
      .filter((col("marks") >= 0) & (col("marks") <= 100))
      .withColumn(
          "result",
          when(col("marks") >= 40, "PASS").otherwise("FAIL")
      )
)

summary = (
    clean.groupBy("department")
         .agg(avg("marks").alias("average_marks"))
)

clean.show()
summary.show()

clean.write.mode("overwrite").parquet("output/clean_students")
summary.write.mode("overwrite").option("header", True).csv("output/summary")

spark.stop()
```

---

# Best Practices

- Prefer DataFrames and Spark SQL for structured data.
- Prefer built-in Spark functions before Python UDFs.
- Avoid `collect()` on large datasets.
- Use explicit schemas in serious pipelines.
- Use Parquet for analytics workloads when appropriate.
- Filter unnecessary data early.
- Select only the columns you need.
- Check partitions instead of blindly using one partition.
- Cache only reused expensive data.
- Use `explain()` to understand execution plans.
- Keep code in Git and commit small logical changes.
- Never commit passwords, private keys or cloud credentials.

---

# Repository Files

```text
py-spark-/
|
|-- README.md
|-- pyspark_practical.py
|-- requirements.txt
|-- .gitignore
|
+-- data/
    |-- students.csv
    +-- students.json
```

---

# Official Documentation

- Apache Spark: https://spark.apache.org/
- PySpark Installation: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
- PySpark DataFrame Quickstart: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
- Spark SQL Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Structured Streaming: https://spark.apache.org/docs/latest/streaming/index.html
- MLlib: https://spark.apache.org/docs/latest/ml-guide.html

---

## Final Goal

After completing this repository, you should understand this complete flow:

```text
Raw Data
   |
   v
PySpark Read
   |
   v
DataFrame
   |
   v
Clean + Transform
   |
   v
Join + Aggregate + SQL
   |
   v
Optimize
   |
   v
Write Output
   |
   v
Local / EC2 / Spark Cluster
```

**Build. Learn. Improve. Repeat.**
