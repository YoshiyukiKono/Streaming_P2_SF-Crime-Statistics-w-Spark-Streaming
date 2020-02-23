

## Implement Apache Spark Structured Streaming

## Set up SparkSession in local mode and Spark configurations.

Use various configs in SparkSession, set up for local mode (using local[*]) and Spark readstream configurations. 
The student should be using at least format (‘kafka’), ‘kafka.bootstrap.servers’, and ‘subscribe’. 
Other configuration properties such as ‘startingOffsets’ and ‘maxRatePerPartition’ are optional. 
This should act as a broker internally tied with Spark, and the student should be able to consume the data that they generated from the local Kafka server to the Spark Structured Streaming.
