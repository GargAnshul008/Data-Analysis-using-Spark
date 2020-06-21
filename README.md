# Data-Analysis-using-Spark
Analyzing data on local computer and Dumbo Cluster (powered by NYU) using Spark

### What to turn in

For this assignment, you will edit the '<b>responses.txt</b>' file, answering several questions listed
out below. Please make sure to include the correct output along with your response.


## 1. Getting started with pyspark

For this section, use `pyspark` on your own local environment (not on dumbo).

The `pyspark` console is much like the standard Python console, except that it
automatically instantiates a spark session for you, which you can access through the
`spark` object.


### Loading data
The project repository comes with several data files in various formats.  As a first
step, load the file `boats.txt` into a DataFrame by executing the following command:

```python
>>> boats = spark.read.csv('boats.txt')
```
The first line loads the file `boats.txt` as a comma-separated values (CSV) file.

Similarly, you can load JavaScript object notation (JSON) files:

```python
>>> sailors = spark.read.json('sailors.json')
>>> reserves = spark.read.json('reserves.json')
```

Once a DataFrame is created, you can print its contents by calling the `show()`
method:
```python
>>> boats.show()
>>> sailors.show()
>>> reserves.show()
```
and you can print its (inferred) schema by calling the `printSchema()` method:
```python
>>> boats.printSchema()
>>> sailors.printSchema()
>>> reserves.printSchema()
```

You'll notice that the data loaded from JSON files (`sailors` and `reserves`) have
type information and proper column headers, while the data loaded from CSV has
string type on all columns and no column headers.  *Why might this be?*

You can fix this by specifying the schema for `boats` on load:
```python
>>> boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')
>>> boats.show()
>>> boats.printSchema()
```
After providing the schema, you should now see that the `boats` DataFrame has a
correct type for each column, and proper column names.


### Creating views

Once you have DataFrames loaded, you can register them as temporary views in your
spark session:
```python
>>> boats.createOrReplaceTempView('boats')
>>> sailors.createOrReplaceTempView('sailors')
>>> reserves.createOrReplaceTempView('reserves')
```
Remember that a `view` in RDBMS terms is a relation that gets constructed at
run-time.
Registering your DataFrames as views will make it possible to execute SQL queries just as you would in a standard RDBMS:
```python
>>> results = spark.sql('SELECT * FROM boats')
```
This creates a new DataFrame object `results`, but remember that all computation in
Spark is lazy: no data will be processed until you ask for it!  For example, you can
print the results:
```python
>>> results.show()
```
or iterate over each row:
```python
>>> for row in results.collect():
...     print(row)
```

### SQL queries and DataFrame methods

As you've seen above, Spark-SQL makes it possible to use SQL to process dataframes without having a proper RDBMS like SQLite or Postgres.
You can also manipulate DataFrames directly from Python.  For example, the following are equivalent:
```python
>>> res1 = spark.sql('SELECT * FROM sailors WHERE rating > 7')
>>> res2 = sailors.filter(sailors.rating > 7)
```
While any query is possible using either interface, some things will be more naturally expressed in SQL, and some things will be easier in Python.
Having some fluency with writing SQL will make it easier to know when to use each interface.


- **Question 1**: how would you express the following computation using SQL instead
  of the object interface?  `sailors.filter(sailors.age > 30).select(sailors.sname)`

- **Question 2**: how would you express the following using the object interface
  instead of SQL?  `spark.sql('SELECT * from reserves WHERE sid != 22')`

- **Question 3**: Using SQL and (multiple) inner joins, in a single query, how many
  distinct boats did each sailor reserve?  The resulting DataFrame should include
  the sailor's id, name, and the count of distinct boats.  (*Hint*: you may need to use `first(...)` aggregation function on some columns.)  Provide both your query and the resulting DataFrame in your response to this question.



## 3. Bigger datasets

In this section, you will use Spark to analyze the same data as in the previous lab assignment (Hadoop).
When doing this part of the assignment, think back about how you implemented these computations as map-reduce programs.


### CSV files

In the project repository, you will again find CSV files `artist_terms.csv` and
`tracks.csv` as given in the previous assignment.

As a first step, load these files as spark DataFrames with proper schema.
Specifically, the `artist_terms` file should have columns

- `artistID`
- `term`

and the `tracks` file should have columns

- `trackID`
- `title`
- `release`
- `year`
- `duration`
- `artistID`

*Note*: You may need to look at the first few lines of each file to determine the column types!


- **Question 4**: Repeating the analysis from Lab2, implement a query using Spark
  which finds for each artist ID, the maximum track year, average track duration, and
  number of terms applied to the artist.  What are the results for the ten artists
  with the longest average track durations?  Include both your query code and
  resulting DataFrame in your response.

- **Question 5**: Create a query that finds the number of distinct tracks associated
  (through artistID) to each term.  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.  Include each query in your response.  What are the 10 most and least popular terms?

### Spark on Dumbo and HDFS

For the last part of this assignment, you will run spark on the dumbo cluster instead
of your own machine.  Remember to follow the environment setup instructions above
when launching `pyspark`.

- **Question 6**: Repeat questions 4 and 5, but now using the large versions of the
  CSV files stored at `hdfs:/user/bm106/pub/artist_term_large.csv` and
  `hdfs:/user/bm106/pub/tracks_large.csv`.  Report the resulting DataFrames in your
  response.  Did you have to change any of your analysis code, and if so, what?
