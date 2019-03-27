package net.davidbuccola.commons.spark

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableInput}
import org.apache.avro.io.DatumReader
import org.apache.avro.mapreduce.AvroRecordReaderBase
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}

import scala.reflect.{ClassTag, classTag}

/**
  * A [[FileInputFormat]] for AVRO which has a couple important characteristics useful
  * for Spark streaming:
  * <p>
  * 1) A new [[T]] instance is created for each datum (as opposed to reusing the instance). This means the [[T]]
  * instance can be held for a while without fear of being overwritten. This is particularly useful when sets of records
  * are gathered up for processing further down the Spark pipeline.
  * <p>
  * 2) The schema is obtained from the datum class rather than the Hadoop configuration.
  */
class AvroFileInputFormat[T <: SpecificRecord : ClassTag] extends FileInputFormat[T, NullWritable] {

  private val datumClass = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  private val schema =
    try
      datumClass.getDeclaredMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
    catch {
      case e: Exception =>
        throw new IllegalArgumentException("Failed to get schema for datum class", e)
    }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new AvroRecordReader

  class AvroRecordReader extends AvroRecordReaderBase[T, NullWritable, T](schema) {

    override def getCurrentKey: T = getCurrentRecord

    override def getCurrentValue: NullWritable = NullWritable.get

    override def createAvroFileReader(input: SeekableInput, datumReader: DatumReader[T]): DataFileReader[T] = new DataFileReader[T](input, datumReader) {

      override def next(reuse: T): T = super.next(null.asInstanceOf[T]) // Don't reuse the buffer even though the caller wanted us to
    }
  }

}
