package com.spark.copyjob

import java.util.UUID

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone }
import com.spark.copyjob._
import scala.collection.JavaConversions._
import java.lang.Long
import java.math.BigInteger;

// 1. Build: sbt assembly
// 2. Run: dse spark-submit --class com.spark.copyjob.SparkCopyJob <path>/Spark-Copy-Job-assembly-1.0.jar

object SparkCopyJob extends App {

  val sparkConf = new SparkConf().setAppName("SparkCopyJob")
  val sc = new SparkContext(sparkConf)

  val sparkCopyJob = new SparkCopyJob

  sparkCopyJob.run(sc)

  // Bring down this compute instance
  sc.stop()

}

class SparkCopyJob {

  import com.datastax.spark.connector._
  import org.apache.spark.SparkContext._

  def run(sc: SparkContext): Unit = {
    var host = sc.broadcast(sc.getConf.get("spark.cassandra.connection.host"));
    val partitions =
      SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("10")), BigInteger.valueOf(SplitPartitions.MIN_PARTITION), BigInteger.valueOf(SplitPartitions.MAX_PARTITION));

    partitions.foreach(partition => {
      val subPartitions =
        SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("1000")), BigInteger.valueOf(partition.getMin()), BigInteger.valueOf(partition.getMax()));
      val parts = sc.parallelize(subPartitions.toSeq,subPartitions.size);

      parts.foreach(part => {

        CopyJobSession.getInstance(host.value).getDataAndInsert(part.getMin, part.getMax)
        
      })

      println(parts.collect.tail)

    })

  }

}
 