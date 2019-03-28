package net.davidbuccola.commons.spark

import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * A factory for [[DStream]] of AVRO records retrieved from files in a directory.
  */
object AvroFileDStreamFactory {

  /**
    * Creates a [[DStream]] of AVRO records from files in a monitored directory.
    */
  def avroFileDStream[T <: SpecificRecord : ClassTag, F <: AvroFileInputFormat[T] : ClassTag](ssc: StreamingContext, directory: String): DStream[T] = {
    ssc.fileStream[T, NullWritable, F](directory)
      .mapPartitions(tuples => tuples.map(_._1))
  }

  /**
    * Creates a [[DStream]] of AVRO records from files in a monitored directory.
    */
  def avroFileDStream[T <: SpecificRecord : ClassTag, F <: AvroFileInputFormat[T] : ClassTag](ssc: StreamingContext, directory: String, filter: Path => Boolean, newFilesOnly: Boolean): DStream[T] = {
    ssc.fileStream[T, NullWritable, F](directory, filter, newFilesOnly)
      .mapPartitions(tuples => tuples.map(_._1))
  }

  /**
    * Creates a [[DStream]] of AVRO records from files in a monitored directory.
    */
  def avroFileDStream[T <: SpecificRecord : ClassTag, F <: AvroFileInputFormat[T] : ClassTag](ssc: StreamingContext, directory: String, filter: Path => Boolean, newFilesOnly: Boolean, configuration: Configuration): DStream[T] = {
    ssc.fileStream[T, NullWritable, F](directory, filter, newFilesOnly, configuration)
      .mapPartitions(tuples => tuples.map(_._1))
  }
}
