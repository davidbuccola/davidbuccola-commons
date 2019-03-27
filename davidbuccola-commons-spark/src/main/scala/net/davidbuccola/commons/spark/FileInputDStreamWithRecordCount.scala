package net.davidbuccola.commons.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/**
  * A variation of [[org.apache.spark.streaming.dstream.FileInputDStream]] which properly reports the record count.
  */
class FileInputDStreamWithRecordCount[K: ClassTag, V: ClassTag, F <: InputFormat[K, V] : ClassTag](
  ssc: StreamingContext,
  directory: String,
  filter: Path => Boolean = FileInputDStreamWithRecordCount.defaultFilter,
  newFilesOnly: Boolean = true,
  configuration: Option[Configuration] = None

) extends FileInputDStreamExtensionHack[K, V, F](ssc, directory, filter, newFilesOnly, configuration, classTag[K], classTag[V], classTag[F]) {

  override def compute(validTime: Time): Option[RDD[(K, V)]] = {

    val counters = new ListBuffer[CountingIterator[(K, V)]]

    def aggregateCount(): Long = counters.foldLeft[Long](0) { (accumulator, counter) => accumulator + counter.count }

    // Report stream input info that includes the record count
    reportInputInfo(new StreamInputInfoWithFutureRecordCount(getNewInputStreamId, aggregateCount), validTime)

    // Return a mapped RDD that accumulates the record count as we go.
    super.compute(validTime).map(rdd =>
      rdd.mapPartitions(partitionRecords => {
        val counter = new CountingIterator[(K, V)](partitionRecords)
        counters.append(counter)
        counter
      }, preservesPartitioning = true))
  }
}

object FileInputDStreamWithRecordCount {

  def defaultFilter(path: Path): Boolean = !path.getName.startsWith(".")
}

private class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
  private var accumulator = 0

  private def isFullyConsumed: Boolean = !iterator.hasNext

  def hasNext(): Boolean = iterator.hasNext

  def count(): Long = accumulator

  def next(): T = {
    accumulator += 1
    iterator.next()
  }
}
