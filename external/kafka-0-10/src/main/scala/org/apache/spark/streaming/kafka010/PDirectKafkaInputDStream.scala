package org.apache.spark.streaming.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._
import scala.util.Random

private[spark] class PDirectKafkaInputDStream[K, V] (_ssc: StreamingContext,
                                                     locationStrategy: LocationStrategy,
                                                     consumerStrategy: ConsumerStrategy[K, V],
                                                     ppc: PerPartitionConfig,
                                                     tpc: Map[String, Int])
  extends DirectKafkaInputDStream[K, V] (_ssc, locationStrategy, consumerStrategy, ppc) {

  val topicAllocationBracket: Map[String, AllocationBracket] = {
    var tab = scala.collection.mutable.Map[String, AllocationBracket]()
    var cumulative = 0
    for ((k, v) <- tpc) {
      tab += (k -> new AllocationBracket(cumulative + 1, cumulative + v))
      cumulative += v

      if (cumulative > 100) {
        throw new IllegalArgumentException("Total share for topics exceeds 100 per cent")
      }
    }
    tab.toMap
  }

  override protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer()
    paranoidPoll(c)
    val parts = c.assignment().asScala

    // make sure new partitions are reflected in currentOffsets
    val newPartitions = parts.diff(currentOffsets.keySet)

    // Check if there's any partition been revoked because of consumer rebalance.
    val revokedPartitions = currentOffsets.keySet.diff(parts)
    if (revokedPartitions.nonEmpty) {
      throw new IllegalStateException(s"Previously tracked partitions " +
        s"${revokedPartitions.mkString("[", ",", "]")} been revoked by Kafka because of consumer " +
        s"rebalance. This is mostly due to another stream with same group id joined, " +
        s"please check if there're different streaming application misconfigure to use same " +
        s"group id. Fundamentally different stream should use different group id")
    }

    // position for new partitions determined by auto.offset.reset if no commit
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap

    val random = new Random().nextInt(101)
    val offsetsToConsider =
      currentOffsets.keySet
        .filter(tp => (random >= topicAllocationBracket(tp.topic()).start() &&
          random <= topicAllocationBracket(tp.topic()).end()))

    // find latest available offsets
    c.seekToEnd(offsetsToConsider.asJava)
    return parts.map(tp => tp -> c.position(tp)).toMap
  }
}
