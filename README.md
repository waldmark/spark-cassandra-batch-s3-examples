## Spark Casaandra Example
![Alt text](images/spark-logo-trademark.png)


<br/>
</br>
<p>
Java example of Apache Spark consuming and processing 911 calls stored in Cassandra. 
</p>

<i>Requirements:</i>
* Java 8 installed
* Cassandra
* Scality S3 server

This demo was developed using docker images running locally for Cassandra and Scality S3. Other instances of Cassandra and S3 should work as well. 

The example can be run from an IDE (like IntelliJ), or from a runnable jar. See instructions below on building the runnable <i>uber-jar</i>.

### Stand alone processing from a file
The class com.objectpartners.spark.rt911.standalone.MainApplication has a runnable main. It loads data into Cassandra;
once loaded, it uses the Spark Cassandra Connector to read and then analyze data from Cassandra, and then store the results into S3.

## Building a runnable jar
A standalone jar can be created using Gradle. In the project root directory, in a terminal run gradle:

1. gradle clean build
2. gradle shadowjar

The uber-jar will be built and placed in the {$project.dir}/build/libs directory.

## Resources
In src/main/resources are two gzips containing 911 call data in csv format:

1. Seattle_Real_Time_Fire_911_Calls_10_Test.csv.gz contains 10 911 calls (10 lines) and can be used for simple testing.
    Note that the application assumes the first line contains header data, so only 9 calls are actually processed.
2. Seattle_Real_Time_Fire_911_Calls_Chrono.csv.gz 
    A chronologically ordered set of (lots of) calls.
    










 

