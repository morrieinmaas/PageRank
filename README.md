### Authors:
###  Moritz Schlichting - 2349638S
###  Alexandros Mina - 2278248M

#### Course: Big Data

#### Exercise 2: PageRank algorithm implemented in Spark.

This is a brief description of the iterative page ranking algorithm implemented in Java programming language using Apache Spark open-source cluster- computing framework. 
In this exercise the specification is very straightforward and hence, couple of assumptions are taken. This is also benefits our implementation since this specification is very close to our design decisions and assumptions taken in Exercise 1 (i.e. 3rd scenario of simple graph).


#### Input preparation:

The purpose of this phase is to get from the input text file specified as the first argument in the command line all the most recent article with its out-links. Therefore, the first RDD namely recordsRDD will be used to hold all the revision's records.

1. The separation of each revision's record is done by just creating a configuration object with the delimiter of the text input format. There is no need to write another record reader class implementation from scratch as with the MapReduce. Therefore, since our input file is located in HDFS, the newAPIHadoopFile() method is used and passed one of its parameters the particular configuration with the identical delimiter "\n\n" to separate properly each revision record. Finally, a key-value pair is returned with the content stored as a value, that's why the map() transformation is used.

2. Second transformation used is the filter() and this is basically, one of the new features of this exercise. The use of this operation is to filter-out any revision that is created after the specified date passed as a fourth argument in the execution command.

3. Next, this step is very similar with one of the mapper jobs in MapReduce implementation. The goal in this state, is to generate a key-value pair of each revision by passing only the information that is going to be used (i.e. Revision ID, Article title, MAIN line/outlinks). Thus, the mapToPair() transformation is used. The article's title is passed as a key and the revision ID is concatenated with the MAIN line (separated by '¬' delimeter) as a value. The reason of this decision is to determine the most recent version (i.e. higher revision ID) of each article.

4. In this step, the reduceByKey() transformation is used to combine together all the revisions of each article and reduce them by getting only the latest one.

5. Then, this is the turn of tokenization. Using the flatToMapPair() transformation the value of each pair is splitted and an iterable list consists of all outlinks of each article is returned.

6. Since one of our assumptions in the specification is to eliminate any duplicate outlink the distinct() transformation is used.

7. Also, the self-loops (i.e any pair with the same key, value) are eliminated using filter() transformation. In this operation, the elimination of any pair that ends with "¬MAIN¬ is also eliminated.

8. Finally, the groupByKey() operation is used to group together each article with its outlinks. Therefore, because the "recordsRDD" will be used to access for each iteration it is a very cheap and efficient desing decision in terms of communication network traffic to hash partition this RDD by partitionBy() transformation followed by the cache() operation to keep it in RAM since it is as static and it will be used to read its content for each iteration. This will reduce the amount of data read and transferred over the network.


#### PageRank algorithm:

In the second part of the program, the PageRank algorithm took place. Two new RDDs are created; one for ranks, and one for contributions.

1. The first transformation mapValues() is used to initialize the page rank score for each article to 1.0 using the recordsRDD. The idea behind this operation instead of map() is to avoid any disruption of the partioning of the recordsRDD.

2. Next, we join() the records and ranks RDDs together to get each article with its outlinks (i.e. using values()) and current rank, and therefore, using flatMaptoPair() transformation the contribution for each article's neighbors is calculated (i.e. current page rank devided by the number of the outlinks) and is ready to be sent to its neighbors.

3. After we calculate the contribution of each article, we performed reduceByKey() follow by mapValues().


#### Output preparation:

The third and the last phase has to do with output format. Based on the specification our output should be sorted in descending order of the page rank score. In order to achieve this, we first swap each pair using the mapToPair() tansformation then we performed sortBykey() and then again mapToPair() to swap the pairs. Finally we eliminate the parentheses around the key-value pair, then we separated it by a single comma using map(), and finally we save the file as a text using saveAsTextFile() to the specified path as it is the second argument of the command line.
