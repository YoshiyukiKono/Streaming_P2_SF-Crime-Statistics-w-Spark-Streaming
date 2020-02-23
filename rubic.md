

## Implement Apache Spark Structured Streaming

## Set up SparkSession in local mode and Spark configurations.

Use various configs in SparkSession, set up for local mode (using local[*]) and Spark readstream configurations. 
The student should be using at least format (‘kafka’), ‘kafka.bootstrap.servers’, and ‘subscribe’. 
Other configuration properties such as `startingOffsets` and `maxRatePerPartition` are optional. 
This should act as a broker internally tied with Spark, and the student should be able to consume the data that they generated from the local Kafka server to the Spark Structured Streaming.


## Write various statistical analytics using Spark Structured Streaming APIs, and analyze and monitor the status of the current micro-batch through progress reporter.

Aggregator/Join logic should show **different types of crimes occurred in certain time frames** (using **window functions**, `group by` original_crime_time), and **how many calls occurred in certain time frames** (`group by` on certain time frame, but the student will need to create **a user defined function internally for Spark to change time format first to do this operation**). 
Any aggregator or join logic should be performed with **Watermark API**. 
The students can play around with the time frame - this is part of their analytical exercises and data exploration.

Student demonstrates in their response to question 2 that they understand, given a resource and system, how to find out the most optimal configuration for their application.

The screenshot of the progress reporter demonstrates that the student correctly ingested the data in a micro-batch fashion and was able to monitor each micro-batch through the console.

### Suggestions to Make Your Project Stand Out!
- Implement FFT (Fast Fourier transform) algorithm to generate the frequency of SF service calls.
- Try to scale out and figure out the combinations on your local machine number of offsets/partitions. Can you make 2000 offsets per trigger? How can you tell you’re ingesting <offsets> per trigger? What tools can you use?
- Generate dynamic charts/images per batch.
