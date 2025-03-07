# PySpark Notes

## Introduction to PySpark
PySpark is the Python API for Apache Spark, an open-source distributed computing system that provides an easy-to-use, fast, and scalable framework for big data processing. It enables efficient large-scale data processing using distributed computing, making it a popular choice for big data analytics and machine learning.

## Installing PySpark
```sh
pip install pyspark
```

## Creating a Spark Session
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()
```

## Creating DataFrames
### From a Python List
```python
from pyspark.sql import Row

data = [Row(name="Alice", age=25), Row(name="Bob", age=30)]
df = spark.createDataFrame(data)
df.show()
```
### From a CSV File
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show()
```

## DataFrame Operations
### Selecting Columns
```python
df.select("name", "age").show()
```

### Filtering Data
```python
df.filter(df.age > 25).show()
```

### Grouping and Aggregation
```python
df.groupBy("age").count().show()
```

### Adding a New Column
```python
df = df.withColumn("new_col", df.age * 2)
df.show()
```

### Renaming Columns
```python
df = df.withColumnRenamed("age", "years")
df.show()
```

### Dropping Columns
```python
df = df.drop("new_col")
df.show()
```

### Sorting Data
```python
df.orderBy(df.age.desc()).show()
```

### Replacing Values
```python
df = df.replace("Alice", "Alicia", "name")
df.show()
```

### Handling Missing Data
#### Dropping Null Values
```python
df.na.drop().show()
```

#### Filling Missing Values
```python
df.na.fill({"age": 0, "name": "Unknown"}).show()
```

## SQL Queries with PySpark
```python
df.createOrReplaceTempView("people")
sql_df = spark.sql("SELECT name, age FROM people WHERE age > 25")
sql_df.show()
```

## Window Functions
Window functions allow performing calculations across a set of rows related to the current row.

### Defining a Window Spec
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

window_spec = Window.partitionBy("department").orderBy("salary")
```

### Row Number
```python
df.withColumn("row_number", row_number().over(window_spec)).show()
```

### Rank
```python
df.withColumn("rank", rank().over(window_spec)).show()
```

### Dense Rank
```python
df.withColumn("dense_rank", dense_rank().over(window_spec)).show()
```

## Partitioning in PySpark
Partitioning helps in performance optimization by distributing data across multiple partitions.

### Checking Current Partitions
```python
print(df.rdd.getNumPartitions())
```

### Repartitioning Data
```python
df = df.repartition(4)
```

### Writing Data in Partitions
```python
df.write.partitionBy("category").parquet("output_path")
```

## Working with RDDs
### Creating an RDD
```python
data = ["apple", "banana", "cherry"]
rdd = spark.sparkContext.parallelize(data)
```

### Transformations
#### Map
```python
rdd_upper = rdd.map(lambda x: x.upper())
print(rdd_upper.collect())
```

#### Filter
```python
rdd_filtered = rdd.filter(lambda x: "a" in x)
print(rdd_filtered.collect())
```

#### FlatMap
```python
data = ["hello world", "pyspark is fun"]
rdd = spark.sparkContext.parallelize(data)
flat_rdd = rdd.flatMap(lambda x: x.split(" "))
print(flat_rdd.collect())
```

#### ReduceByKey
```python
data = [("a", 1), ("b", 2), ("a", 3)]
rdd = spark.sparkContext.parallelize(data)
rdd_reduced = rdd.reduceByKey(lambda x, y: x + y)
print(rdd_reduced.collect())
```

### Actions
```python
print(rdd.count())
print(rdd.collect())
```

## Writing DataFrames to Files
### Writing to CSV
```python
df.write.csv("output.csv", header=True)
```
### Writing to JSON
```python
df.write.json("output.json")
```

## Machine Learning with PySpark (MLlib)
### Importing ML Libraries
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
```
### Preparing Data
```python
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df_transformed = assembler.transform(df)
```
### Training a Model
```python
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(df_transformed)
```
### Making Predictions
```python
predictions = model.transform(df_transformed)
predictions.show()
```
