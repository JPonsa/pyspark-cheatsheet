# 🐍 📄 PySpark Cheat Sheet

A quick reference guide to the most commonly used patterns and functions in PySpark SQL.

#### Table of Contents
- [Quickstart](#quickstart)
- [Basics](#basics)
- [Common Patterns](#common-patterns)
    - [Importing Functions & Types](#importing-functions--types)
    - [Filtering](#filtering)
    - [Joins](#joins)
    - [Column Operations](#column-operations)
    - [Casting & Coalescing Null Values & Duplicates](#casting--coalescing-null-values--duplicates)
- [Null values](#Null-values)
- [String Operations](#string-operations)
    - [String Filters](#string-filters)
    - [String Functions](#string-functions)
- [Number Operations](#number-operations)
- [Date & Timestamp Operations](#date--timestamp-operations)
- [Array Operations](#array-operations)
- [Struct Operations](#struct-operations)
- [Aggregation Operations](#aggregation-operations)
- [Advanced Operations](#advanced-operations)
    - [Partition](#patition)     
    - [Repartitioning](#repartitioning)
    - [UDFs (User Defined Functions](#udfs-user-defined-functions)
- [Useful Functions / Tranformations](#useful-functions--transformations)
- [Define Schema]
- [Resilient Distributed Datasets (RDDs)]
- [Working with Delta Files](#Working-with-Delta-Files)
- [pyspark Pandas](#Pandas)
- [Run SQL](#Run_SQL)
- [Other sources]

If you can't find what you're looking for, check out the [PySpark Official Documentation](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and add it here!

## Quickstart

Install on macOS:

```bash
brew install apache-spark && pip install pyspark
```

Create your first DataFrame:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# I/O options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
df = spark.read.csv('/path/to/your/input/file')
df = spart.read.option('header','true').csv('/path/to/your/input/file', inferSchema=True) # use 1st row as header and infer data type, otherwise all string
df = spart.read.csv('/path/to/your/input/file', header=True, inferSchema=True)
```

## Basics

```python
# Show a preview
df.show()

# Show preview of first / last n rows
df.head(5)
df.tail(5)

# Show preview as JSON (WARNING: in-memory)
df = df.limit(10) # optional
print(json.dumps([row.asDict(recursive=True) for row in df.collect()], indent=2))

# Limit actual DataFrame to n rows (non-deterministic)
df = df.limit(5)

# Get columns
df.columns

# Get columns + column types
df.dtypes
df.describe()

# Get schema
df.schema
df.printSchema()
# Get row count
df.count()

# Get column count
len(df.columns)

# Write output to disk
df.write.csv('/path/to/your/output/file')
df.write.parquet('/path/to/your/output/file')

# Diffrent modes to write data. append / overwrite / error / ignore
# See: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html

df.write.mode('append').parquet(...)

# partition
df.write.mode('overwrite').partitionBy('year').parque('/path/to/file')

# Get results (WARNING: in-memory) as list of PySpark Rows
df = df.collect()

# Get results (WARNING: in-memory) as list of Python dicts
dicts = [row.asDict(recursive=True) for row in df.collect()]

# Convert (WARNING: in-memory) to Pandas DataFrame
df = df.toPandas()
```

### JSON Schema -  Multiple levels

```python
name_schema = T.StructType(fields=[
    T.StructField("forename", T.StringType(), True),
    T.StructField("surname", T.StringType(), True)
])

drivers_schema = T.StructType(fields=[
    T.StructField("driverId", T.IntegerType(), False),
    T.StructField("driverRef", T.StringType(), True),
    T.StructField("number", T.IntegerType(), True),
    T.StructField("code", T.StringType(), True),
    T.StructField("name", name_schema),
    T.StructField("nationality", T.StringType(), True),
    T.StructField("url", T.StringType(), True)
])
                            
drivers_df  = spark.read.json(f"{raw_abfs_driver}/drivers.json", 
                              schema=drivers_schema)

```


## Common Patterns

#### Importing Functions & Types

```python
# Easily reference these as F.my_function() and T.my_type() below
from pyspark.sql import functions as F, types as T
```

#### Filtering

```python
# Filter on equals condition
df = df.filter(df.is_adult == 'Y')

# Filter on >, <, >=, <= condition
df = df.filter(df.age > 25)
df =  df.filter(df["age"] = 25)
df = df.filter("Salary <= 2000").select(["Name, "age])

# Multiple conditions require parentheses around each condition
df = df.filter((df.age > 25) & (df.is_adult == 'Y'))
df = df.filter("age > 15 and is_adult = 'Y'")

# Compare against a list of allowed values
df = df.filter(F.col('first_name').isin([3, 4, 7]))

# Sort results
df = df.orderBy(df.age.asc())) # Equivalent to pandas df.sort_values(by='age')
df = df.orderBy(df.age.desc()))
```

#### Join

```python
# Left join in another dataset
df = df.join(person_lookup_table, 'person_id', 'left')

# Match on different columns in left & right datasets
df = df.join(other_table, df.id == other_table.person_id, 'left')

# Match on multiple columns
df = df.join(other_table, ['first_name', 'last_name'], 'left')


# Semi Joins
# Same as inner join but getting only the columns from the left table

# anti-join
# Any thing on the left table not present in the right.
df = df.join(other_table, df.id == other_table.person_id, 'anti')

# Cross Join
crossjoin_df = left_df.crossJoin(right_df)
```

#### Union
```python
df.uniom(df2) # Equicalent to pd.concat([df, df2])
```

#### Column Operations

```python
# Add a new static column
df = df.withColumn('status', F.lit('PASS'))
df = df.withColumn('One', F.lit(1))

# Construct a new dynamic column
df = df.withColumn('full_name', F.when(
    (df.fname.isNotNull() & df.lname.isNotNull()), F.concat(df.fname, df.lname)
).otherwise(F.lit('N/A'))

# Pick which columns to keep, optionally rename some
df = df.select(
    'name',
    'age',
    F.col('dob').alias('date_of_birth'),
)

# Select column
df.select('name') # Option 1
df.select(F.col('name') #  Option 2
df.select(df.name) # Option 3

# Select multiple columns
df.select(['name','age'])

# Remove columns
df = df.drop('mod_dt', 'mod_username')

# Rename a column
df = df.withColumnRenamed('dob', 'date_of_birth')

# Keep all the columns which also occur in another dataset
df = df.select(*(F.col(c) for c in df2.columns))

# apply function e.g. min and max
df.select(F.min(F.col('Date')), F.max(F.col('Date'))).show(1)

# Batch Rename/Clean Columns
for col in df.columns:
    df = df.withColumnRenamed(col, F.col.lower().replace(' ', '_').replace('-', '_'))
```

#### Casting & Coalescing Null Values & Duplicates

```python
# Cast a column to a different type
df = df.withColumn('price', df.price.cast(T.DoubleType()))

# Replace all nulls with a specific value
df = df.fillna({
    'first_name': 'Tom',
    'age': 0,
})

# Take the first value that is not null
df = df.withColumn('last_name', F.coalesce(df.last_name, df.surname, F.lit('N/A')))

# Drop duplicate rows in a dataset (distinct)
df = df.dropDuplicates() # or
df = df.distinct()

# Drop duplicate rows, but consider only specific columns
df = df.dropDuplicates(['name', 'height'])

# Replace empty strings with null (leave out subset keyword arg to replace in all columns)
df = df.replace({"": None}, subset=["name"])

# Convert Python/PySpark/NumPy NaN operator to null
df = df.replace(float("nan"), None)
```

## String Operations

#### String Filters

```python
# Contains - col.contains(string)
df = df.filter(df.name.contains('o'))

# Starts With - col.startswith(string)
df = df.filter(df.name.startswith('Al'))

# Ends With - col.endswith(string)
df = df.filter(df.name.endswith('ice'))

# Is Null - col.isNull()
df = df.filter(df.is_adult.isNull())

# Is Not Null - col.isNotNull()
df = df.filter(df.first_name.isNotNull())

# Like - col.like(string_with_sql_wildcards)
df = df.filter(df.name.like('Al%'))

# Regex Like - col.rlike(regex)
df = df.filter(df.name.rlike('[A-Z]*ice$'))

# Is In List - col.isin(*cols)
df = df.filter(df.name.isin('Bob', 'Mike'))
```
### Null-values

```python
df = df.na.drop() # Drop all records with nulls
df = df.na.drop(how='all') # all values in record must be null, by default 'any'
df = df.na.drop(how='any', thresh=2) # only records with 2+ null values are deleted.
df = df.na.drop(how='any', subset=['col1', 'col2']) # only columns 'col1' and 'col2' are considered

df = df.na.fill('Missing value')
df = df.na.fill('Missing value', ['col1', 'col2'])

from pyspark.ml.feature import Imputer

imputer =  Imputer(
    inputCols=['col1', 'col2'],
    outputCols=['{}_imputed'.format(c) for c in ['col1', 'col2]] 
).setStrategy('mean')

imputer.fit(df).transform(df).show()
```

#### String Functions

```python
# Substring - col.substr(startPos, length)
df = df.withColumn('short_id', df.id.substr(0, 10))

# Trim - F.trim(col)
df = df.withColumn('name', F.trim(df.name))

# Left Pad - F.lpad(col, len, pad)
# Right Pad - F.rpad(col, len, pad)
df = df.withColumn('id', F.lpad('id', 4, '0'))

# Left Trim - F.ltrim(col)
# Right Trim - F.rtrim(col)
df = df.withColumn('id', F.ltrim('id'))

# Concatenate - F.concat(*cols)
df = df.withColumn('full_name', F.concat('fname', F.lit(' '), 'lname'))

# Concatenate with Separator/Delimiter - F.concat_ws(delimiter, *cols)
df = df.withColumn('full_name', F.concat_ws('-', 'fname', 'lname'))

# Regex Replace - F.regexp_replace(str, pattern, replacement)[source]
df = df.withColumn('id', F.regexp_replace(id, '0F1(.*)', '1F1-$1'))

# Regex Extract - F.regexp_extract(str, pattern, idx)
df = df.withColumn('id', F.regexp_extract(id, '[0-9]*', 0))
```

## Number Operations

```python
# Round - F.round(col, scale=0)
df = df.withColumn('price', F.round('price', 0))

# Floor - F.floor(col)
df = df.withColumn('price', F.floor('price'))

# Ceiling - F.ceil(col)
df = df.withColumn('price', F.ceil('price'))

# Absolute Value - F.abs(col)
df = df.withColumn('price', F.abs('price'))

# X raised to power Y – F.pow(x, y)
df = df.withColumn('exponential_growth', F.pow('x', 'y'))

# Select smallest value out of multiple columns – F.least(*cols)
df = df.withColumn('least', F.least('subtotal', 'total'))

# Select largest value out of multiple columns – F.greatest(*cols)
df = df.withColumn('greatest', F.greatest('subtotal', 'total'))
```

## Date & Timestamp Operations

```python
# Add a column with the current date
df = df.withColumn('current_date', F.current_date())

# Convert a string of known format to a date (excludes time information)
df = df.withColumn('date_of_birth', F.to_date('date_of_birth', 'yyyy-MM-dd'))

# Convert a string of known format to a timestamp (includes time information)
df = df.withColumn('time_of_birth', F.to_timestamp('time_of_birth', 'yyyy-MM-dd HH:mm:ss'))

df = df.withColumn('race_timestamp', F.to_timestamp(F.concat(F.col('date'), F.lit(' '), F.col('time')), 'yyyy-MM-dd HH:mm:ss')

# Get year from date:       F.year(col)
# Get month from date:      F.month(col)
# Get day from date:        F.dayofmonth(col)
# Get hour from date:       F.hour(col)
# Get minute from date:     F.minute(col)
# Get second from date:     F.second(col)
df = df.filter(F.year('date_of_birth') == F.lit('2017'))

# Add & subtract days
df.select(F.date_add('date_of_birth', 3)).show(1)

df = df.withColumn('three_days_after', F.date_add('date_of_birth', 3))
df = df.withColumn('three_days_before', F.date_sub('date_of_birth', 3))

# Add & Subtract months
df = df.withColumn('next_month', F.add_month('date_of_birth', 1))

# Get number of days between two dates
df = df.withColumn('days_between', F.datediff('start', 'end'))

# Get number of months between two dates
df = df.withColumn('months_between', F.months_between('start', 'end'))

# Keep only rows where date_of_birth is between 2017-05-10 and 2018-07-21
df = df.filter(
    (F.col('date_of_birth') >= F.lit('2017-05-10')) &
    (F.col('date_of_birth') <= F.lit('2018-07-21'))
)
```

## Array Operations

```python
# Column Array - F.array(*cols)
df = df.withColumn('full_name', F.array('fname', 'lname'))

# Empty Array - F.array(*cols)
df = df.withColumn('empty_array_column', F.array([]))

# Get element at index – col.getItem(n)
df = df.withColumn('first_element', F.col("my_array").getItem(0)f)

# Array Size/Length – F.size(col)
df = df.withColumn('array_length', F.size('my_array'))

# Flatten Array – F.flatten(col)
df = df.withColumn('flattened', F.flatten('my_array'))

# Unique/Distinct Elements – F.array_distinct(col)
df = df.withColumn('unique_elements', F.array_distinct('my_array'))
df.select(column).distinct().show() # Equivalent to pandas df.column.unique()

# Map over & transform array elements – F.transform(col, func: col -> col)
df = df.withColumn('elem_ids', F.transform(F.col('my_array'), lambda x: x.getField('id')))

# Return a row per array element – F.explode(col)
df = df.select(F.explode('my_array'))
```

## Struct Operations

```python
# Make a new Struct column (similar to Python's `dict()`) – F.struct(*cols)
df = df.withColumn('my_struct', F.struct(F.col('col_a'), F.col('col_b')))

# Get item from struct by key – col.getField(str)
df = df.withColumn('col_a', F.col('my_struct').getField('col_a'))
```


## Aggregation Operations

```python
# Row Count:                F.count()
# Sum of Rows in Group:     F.sum(*cols)
# Mean of Rows in Group:    F.mean(*cols)
# Max of Rows in Group:     F.max(*cols)
# Min of Rows in Group:     F.min(*cols)
# First Row in Group:       F.alias(*cols)
df = df.groupBy('gender').agg(F.max('age').alias('max_age_by_gender'))

# Collect a Set of all Rows in Group:       F.collect_set(col)
# Collect a List of all Rows in Group:      F.collect_list(col)
df = df.groupBy('age').agg(F.collect_set('name').alias('person_names'))
df = df.groupby("Name").sum()
df = df.agg({"Salary":"sum"})

# Just take the lastest row for each combination (Window Functions)
from pyspark.sql import Window as W

window = W.partitionBy("first_name", "last_name").orderBy(F.desc("date"))
df = df.withColumn("row_number", F.row_number().over(window))
df = df.filter(F.col("row_number") == 1)
df = df.drop("row_number")


driverRankSpec = F.Window.partitionBy("race_year").orderBy(F.desc("total_points"))
df =  df.withColumn("rank", F.rank().over(driverRankSpec))

```

## Advanced Operations

#### Partition
df.write.mode('overwrite').partitionBy('year').parque('/path/to/file')

#### Repartitioning

```python
# Repartition – df.repartition(num_output_partitions)
df = df.repartition(1)
```

#### UDFs (User Defined Functions

```python
# Multiply each row's age column by two
times_two_udf = F.udf(lambda x: x * 2)
df = df.withColumn('age', times_two_udf(df.age))

# Randomly choose a value to use as a row's name
import random

random_name_udf = F.udf(lambda: random.choice(['Bob', 'Tom', 'Amy', 'Jenna']))
df = df.withColumn('name', random_name_udf())
```

## Useful Functions / Transformations

```python
def flatten(df: DataFrame, delimiter="_") -> DataFrame:
    '''
    Flatten nested struct columns in `df` by one level separated by `delimiter`, i.e.:

    df = [ {'a': {'b': 1, 'c': 2} } ]
    df = flatten(df, '_')
    -> [ {'a_b': 1, 'a_c': 2} ]
    '''
    flat_cols = [name for name, type in df.dtypes if not type.startswith("struct")]
    nested_cols = [name for name, type in df.dtypes if type.startswith("struct")]

    flat_df = df.select(
        flat_cols
        + [F.col(nc + "." + c).alias(nc + delimiter + c) for nc in nested_cols for c in df.select(nc + ".*").columns]
    )
    return flat_df


def lookup_and_replace(df1, df2, df1_key, df2_key, df2_value):
    '''
    Replace every value in `df1`'s `df1_key` column with the corresponding value
    `df2_value` from `df2` where `df1_key` matches `df2_key`

    df = lookup_and_replace(people, pay_codes, id, pay_code_id, pay_code_desc)
    '''
    return (
        df1
        .join(df2[[df2_key, df2_value]], df1[df1_key] == df2[df2_key], 'left')
        .withColumn(df1_key, F.coalesce(F.col(df2_value), F.col(df1_key)))
        .drop(df2_key)
        .drop(df2_value)
    )

```

#### Define Schema

```python
from pyspark.sql import types as T

schema = T.StructType([
    T.StrucField('ID', T.StringType, True), # Field ID type str w/ NaN values
    T.StrucField('Date', T.TimestampType, True),
    T.StrucField('Latitude', T.DoubleType, False)
])

df = spark.read.csv('myfile.csv', schema=schema)

```
#### Resilient Distributed Datasets (RDDs)

immutable partition collection of records
Each record is a python object. Pyspark does not recognise the schema of the record
Only use RDD when absolutely necessary.

RDDs cheat sheet:
https://images.datacamp.com/image/upload/v1676303379/Marketing/Blog/PySpark_RDD_Cheat_Sheet.pdf

- Transformations -> returns an RDD
  e.g. 
- Actions -> returns another type than RDD
  e.g. reduce, count

#### Working with Delta files
https://docs.delta.io/latest/quick-start.html#language-python

### Pandas

```python
import pyspark.pandas as ps

psdf = ps.from_pandas(df)
df = psdf.to_pandas(psdf)
```
### Run_SQL
```python
df.createTmpView(v_my_view)
tmp_df = spark.sql("SELECT * FROM v_my_view WHERE race_year = 2029")

```

#### Other Sources
DataCamp
https://www.datacamp.com/cheat-sheet/pyspark-cheat-sheet-spark-dataframes-in-python
