package org.apache.spark.streaming.kafka010

final class AllocationBracket(start: Int,
                              end: Int
                             ) extends Serializable {
  def start() : Int = start
  def end() : Int = end
}
