package net.davidbuccola.commons.spark

import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * A factory for [[DStream]] of AVRO records retrieved from files in a directory.
  * <p>
  * The implementation leverages [[org.apache.spark.streaming.dstream.FileInputDStream]] as well as a few other
  * preexisting classes. The existing classes, however, are used in a slightly different way in order to improve
  * functionality and overcome a major deficiency. Most of the gymnastics are to get a record count that is available in
  * the batch summary. This turns out to be very important for effective performance measure and status. The extra work
  * is needed because the [[org.apache.spark.streaming.dstream.FileInputDStream]] sets the record count to zero. Even if
  * [[org.apache.spark.streaming.dstream.FileInputDStream]] set something non-zero it would be the number of files
  * (which is not particularly useful). With the hacks here, the record count is set to the number of AVRO records.
  */
object AvroFileDStreamFactory {

  /**
    * Creates a [[DStream]] of AVRO records from files in a monitored directory.
    */
  def avroFileDStream[T <: SpecificRecord : ClassTag](
    ssc: StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStreamWithRecordCount.defaultFilter,
    newFilesOnly: Boolean = true,
    configuration: Option[Configuration] = None
  ): DStream[T] = {
    new AvroFileInputDStream[T](ssc, directory, filter, newFilesOnly, configuration).mapPartitions(tuples => tuples.map(tuple => tuple._1))
  }

}