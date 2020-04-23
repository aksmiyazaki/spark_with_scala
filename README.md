# Apache Spark with Scala - Hands On with Big Data!

## Spark 3
    - More stuff being deprecated than new features.
    - Claims to be faster than Spark 2.
    - GraphX.
    - Deprecation of MLLib using RDD`s

## SparkContext
    - Created by the driver program.
    - Responsible for RDDs.
    
## RDDs
    - Fault tolerant.
    - Handles distribution.
    - It is a dataset.

### Transformations
    - map - applies a transformation (function) on each row.
    - flatMAp - applies a transformation (function) on each row, but there's no need to have a 1 to 1 relationship btw the source and result RDD.
    - filter - filters rows given a criteria
    - distinct - distinct rows
    - sample - creates a sample from RDD
    - set operations (union, intersection, subtract, etc.)
    
### Actions
    - collect - get all data from a RDD
    - count - count rows
    - countByValue - count occurences of each unique value in a RDD
    - take - take a number of rows in a RDD
    - top - similar as take
    - reduce - like reduce in mapReduce.

### Job Stages
    - Based on When data needs to be reorganized.
    - Each stage is broken into tasks (which may be distributed across a cluster)
    - Job -> Stages -> Tasks

### Key/Value RDDs
    - RDDs formed by Key, Value pairs (value can be a complex structure)
    - `reduceByKey` - combine values with the same key using some function
    - `groupByKey` - group values with same key
    - `sortByKey` - sort rdd by key values
    - `keys()`, `values()` - create an RDD with just the keys or values.
    - SQL style joins (inner, outer, etc)
    - If just working with values, use `mapValues` and `flatMapValues` (more efficient).

### General notes
    - About optimization -> minimize shuffle and keep things parallelizable
    
    