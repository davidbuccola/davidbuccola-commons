package net.davidbuccola.commons.spark;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroRecordReaderBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * A base class for AVRO {@link FileInputFormat} implementations which have a couple important characteristics useful
 * for Spark streaming:
 * <p>
 * 1) A new {@link T} instance is created for each datum (as opposed to reusing the instance). This means the {@link T}
 * instance can be held for a while without fear of being overwritten. This is particularly useful when sets of records
 * are gathered up for processing further down the Spark pipeline.
 * <p>
 * 2) The schema is obtained from the datum class rather than the Hadoop configuration.
 */
public abstract class AvroFileInputFormat<T extends SpecificRecord> extends FileInputFormat<T, NullWritable> {

    private final Schema schema;

    protected AvroFileInputFormat(Class<T> datumClass) {
        this.schema = getSchema(datumClass);
    }

    @Override
    public RecordReader<T, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new AvroRecordReader();
    }

    private Schema getSchema(Class<T> datumClass) {
        try {
            return (Schema) datumClass.getDeclaredMethod("getClassSchema").invoke(null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to get schema for datum class", e);
        }
    }

    private class AvroRecordReader extends AvroRecordReaderBase<T, NullWritable, T> {

        AvroRecordReader() {
            super(schema);
        }

        @Override
        public T getCurrentKey() throws IOException, InterruptedException {
            return getCurrentRecord();
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        protected DataFileReader<T> createAvroFileReader(SeekableInput input, DatumReader<T> datumReader) throws IOException {
            return new DataFileReader<T>(input, datumReader) {
                @Override
                public T next(T reuse) throws IOException {
                    return super.next(null); // Don't reuse the buffer even though the caller wanted us to
                }
            };
        }
    }
}
