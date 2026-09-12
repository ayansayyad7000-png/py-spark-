"""
PySpark Basic to Advanced Practical
===================================

Run:
    python pyspark_practical.py

or:
    spark-submit --master "local[*]" pyspark_practical.py

This file is intentionally written in simple sections so a beginner can
run it and learn what each PySpark operation does.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dense_rank,
    lit,
    max as spark_max,
    min as spark_min,
    rank,
    row_number,
    upper,
    when,
)
from pyspark.sql.window import Window


def heading(text):
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80)


def create_spark():
    return (
        SparkSession.builder
        .appName("PySparkPractical")
        .master("local[*]")
        .getOrCreate()
    )


def practical_1_dataframe(spark):
    heading("PRACTICAL 1 - CREATE DATAFRAME")

    data = [
        (1, "Ayan", 21, "IT", 82),
        (2, "Sara", 22, "CSE", 91),
        (3, "Ali", 20, "IT", 68),
        (4, "Neha", 21, "CSE", 76),
        (5, "Rahul", 23, "Mechanical", 35),
    ]

    columns = ["id", "name", "age", "department", "marks"]

    df = spark.createDataFrame(data, columns)

    df.show()
    df.printSchema()

    return df


def practical_2_select_filter(df):
    heading("PRACTICAL 2 - SELECT AND FILTER")

    print("Only name and marks:")
    df.select("name", "marks").show()

    print("Students with marks >= 75:")
    df.filter(col("marks") >= 75).show()

    print("IT students age >= 21:")
    df.filter(
        (col("department") == "IT") &
        (col("age") >= 21)
    ).show()


def practical_3_columns(df):
    heading("PRACTICAL 3 - ADD, RENAME AND DROP COLUMNS")

    changed = (
        df.withColumn(
            "result",
            when(col("marks") >= 40, "PASS").otherwise("FAIL")
        )
        .withColumn("bonus_marks", col("marks") + 5)
        .withColumnRenamed("department", "dept")
    )

    changed.show()

    print("Drop bonus_marks:")
    changed.drop("bonus_marks").show()


def practical_4_sort_group(df):
    heading("PRACTICAL 4 - SORT, GROUP AND AGGREGATE")

    print("Highest marks first:")
    df.orderBy(col("marks").desc()).show()

    print("Department summary:")
    (
        df.groupBy("department")
        .agg(
            count("*").alias("student_count"),
            avg("marks").alias("average_marks"),
            spark_min("marks").alias("minimum_marks"),
            spark_max("marks").alias("maximum_marks"),
        )
        .orderBy(col("average_marks").desc())
        .show()
    )


def practical_5_read_csv(spark):
    heading("PRACTICAL 5 - READ CSV")

    csv_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/students.csv")
    )

    csv_df.show()
    csv_df.printSchema()

    return csv_df


def practical_6_clean_data(df):
    heading("PRACTICAL 6 - CLEAN DATA")

    clean_df = (
        df.dropna(subset=["id", "name", "marks"])
        .filter(
            (col("marks") >= 0) &
            (col("marks") <= 100)
        )
        .withColumn(
            "result",
            when(col("marks") >= 40, "PASS").otherwise("FAIL")
        )
    )

    clean_df.show()

    return clean_df


def practical_7_spark_sql(spark, df):
    heading("PRACTICAL 7 - SPARK SQL")

    df.createOrReplaceTempView("students")

    result = spark.sql(
        """
        SELECT
            department,
            COUNT(*) AS total_students,
            ROUND(AVG(marks), 2) AS average_marks
        FROM students
        GROUP BY department
        ORDER BY average_marks DESC
        """
    )

    result.show()


def practical_8_join(spark):
    heading("PRACTICAL 8 - JOIN")

    students = spark.createDataFrame(
        [
            (1, "Ayan", 101),
            (2, "Sara", 102),
            (3, "Ali", 101),
            (4, "Unknown Department Student", 999),
        ],
        ["student_id", "name", "dept_id"],
    )

    departments = spark.createDataFrame(
        [
            (101, "IT"),
            (102, "CSE"),
            (103, "Mechanical"),
        ],
        ["dept_id", "department"],
    )

    print("INNER JOIN:")
    students.join(departments, on="dept_id", how="inner").show()

    print("LEFT JOIN:")
    students.join(departments, on="dept_id", how="left").show()


def practical_9_window(df):
    heading("PRACTICAL 9 - WINDOW FUNCTIONS")

    window_spec = (
        Window
        .partitionBy("department")
        .orderBy(col("marks").desc())
    )

    ranked = (
        df.withColumn("row_number", row_number().over(window_spec))
        .withColumn("rank", rank().over(window_spec))
        .withColumn("dense_rank", dense_rank().over(window_spec))
    )

    ranked.show()


def practical_10_rdd(spark):
    heading("PRACTICAL 10 - RDD BASICS")

    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

    print("Original:", rdd.collect())
    print("Squared:", rdd.map(lambda x: x * x).collect())
    print("Even:", rdd.filter(lambda x: x % 2 == 0).collect())
    print("Total:", rdd.reduce(lambda a, b: a + b))


def practical_11_partitions_cache_explain(df):
    heading("PRACTICAL 11 - PARTITIONS, CACHE AND EXPLAIN")

    print("Current partitions:", df.rdd.getNumPartitions())

    repartitioned = df.repartition(4)
    print("After repartition(4):", repartitioned.rdd.getNumPartitions())

    df.cache()
    print("Row count after cache:", df.count())

    print("\nExecution plan:")
    df.filter(col("marks") > 70).explain("formatted")

    df.unpersist()


def practical_12_functions(df):
    heading("PRACTICAL 12 - BUILT-IN FUNCTIONS")

    (
        df.select(
            "name",
            upper(col("name")).alias("upper_name"),
            "marks",
            when(col("marks") >= 75, "GOOD")
            .otherwise("NEEDS_IMPROVEMENT")
            .alias("performance"),
            lit("PySpark").alias("technology"),
        )
        .show()
    )


def practical_13_write(df):
    heading("PRACTICAL 13 - WRITE PARQUET AND CSV")

    (
        df.write
        .mode("overwrite")
        .parquet("output/students_parquet")
    )

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv("output/students_csv")
    )

    print("Saved:")
    print("  output/students_parquet")
    print("  output/students_csv")


def practical_14_mllib(spark):
    heading("PRACTICAL 14 - MLLIB BASIC LINEAR REGRESSION")

    try:
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression

        data = spark.createDataFrame(
            [
                (1.0, 10.0),
                (2.0, 20.0),
                (3.0, 30.0),
                (4.0, 40.0),
            ],
            ["hours", "marks"],
        )

        assembler = VectorAssembler(
            inputCols=["hours"],
            outputCol="features",
        )

        prepared = assembler.transform(data)

        model = LinearRegression(
            featuresCol="features",
            labelCol="marks",
        ).fit(prepared)

        predictions = model.transform(prepared)

        predictions.select(
            "hours",
            "marks",
            "prediction",
        ).show()

    except Exception as error:
        print("MLlib example could not run.")
        print("Install optional dependencies if required:")
        print('python -m pip install "pyspark[ml]"')
        print("Error:", error)


def streaming_example(spark):
    """
    Optional structured streaming example.

    This function is NOT called automatically because awaitTermination()
    keeps running until you stop it with Ctrl+C.
    """

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


def main():
    spark = create_spark()

    # Reduce Spark console noise for learning.
    spark.sparkContext.setLogLevel("WARN")

    heading("PYSPARK PRACTICAL START")
    print("Spark version:", spark.version)
    print("Application:", spark.sparkContext.appName)
    print("Master:", spark.sparkContext.master)

    df = practical_1_dataframe(spark)
    practical_2_select_filter(df)
    practical_3_columns(df)
    practical_4_sort_group(df)

    csv_df = practical_5_read_csv(spark)
    clean_df = practical_6_clean_data(csv_df)

    practical_7_spark_sql(spark, clean_df)
    practical_8_join(spark)
    practical_9_window(clean_df)
    practical_10_rdd(spark)
    practical_11_partitions_cache_explain(clean_df)
    practical_12_functions(clean_df)
    practical_13_write(clean_df)
    practical_14_mllib(spark)

    heading("DONE")
    print("All normal practicals completed.")
    print("For streaming, call streaming_example(spark) separately.")

    spark.stop()


if __name__ == "__main__":
    main()
