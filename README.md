# spark-copy-job

This code will help you batch copy Cassandra tables using Spark Jobs. This code has rate-limiters which will prevent from copying too fast which can take down a cluster. Also, the retry policy is not implemented, as it is left to the implementer to do that. The advantage of using this vs doing a dataframe copy is that you can iterate through particular partition ranges and copy parts of a table slowly. 


To compile the code simply run: "sbt assembly"
To run the code on DSE: dse spark-submit --class com.spark.copyjob.SparkCopyJob <path>/Spark-Copy-Job-assembly-1.0.jar

** Please node the code will not work as is, as you are expected to fill in the table details in the com.spark.copyjob.CopyJobSession class.
