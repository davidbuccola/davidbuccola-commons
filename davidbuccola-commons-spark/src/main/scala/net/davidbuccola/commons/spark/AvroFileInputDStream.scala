package net.davidbuccola.commons.spark

import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

class AvroFileInputDStream[T <: SpecificRecord : ClassTag] private[spark](
  ssc: StreamingContext,
  directory: String,
  filter: Path => Boolean = FileInputDStreamWithRecordCount.defaultFilter,
  newFilesOnly: Boolean = true,
  configuration: Option[Configuration] = None

) extends FileInputDStreamWithRecordCount[T, NullWritable, AvroFileInputFormat[T]](ssc, directory, filter, newFilesOnly, configuration) {
}
