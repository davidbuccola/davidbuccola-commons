package net.davidbuccola.commons.spark;

import com.google.common.collect.Iterators;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.FileInputDStream;
import org.apache.spark.util.LongAccumulator;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.nio.file.Path;
import java.util.Iterator;

/**
 * A factory for {@link JavaDStream}s of AVRO records that come from a file. At the moment this is just used for testing
 * with batches of sample data.
 * <p>
 * The implementation leverages {@link FileInputDStream} as well as a few other preexisting classes. The existing
 * classes, however, are used in a slightly different way in order to improve functionality and overcome a major
 * deficiency. Most of the gymnastics are to get a record count that is available in the batch summary. This turns out
 * to be very important for effective performance measure and status. The extra work is needed because the {@link
 * FileInputDStream} sets the record count to zero. Even if {@link FileInputDStream} set something non-zero it would be
 * the number of files (which is not particularly useful). With the hacks here, the record count is set to the number of
 * AVRO records.
 */
public final class AvroFileInputDStreamFactory {

    /**
     * Creates a {@link JavaDStream} of AVRO records from files in a monitored directory.
     */
    public static <T extends SpecificRecord> JavaDStream<T> avroFileInputDStream(JavaStreamingContext context, Path directory, Class<T> datumClass, Class<? extends AvroFileInputFormat<T>> formatClass) {
        return avroFileInputDStream(context, directory, datumClass, formatClass, null);
    }

    public static <T extends SpecificRecord> JavaDStream<T> avroFileInputDStream(JavaStreamingContext context, Path directory, Class<T> datumClass, Class<? extends AvroFileInputFormat<T>> formatClass, Configuration hadoopConfig) {
        CountedFileInputDStream<T> fileStream = new CountedFileInputDStream<>(context, directory, datumClass, formatClass, true, Option.apply(hadoopConfig));
        JavaPairInputDStream<T, NullWritable> pairStream = JavaPairInputDStream.fromInputDStream(
            fileStream, ClassTag$.MODULE$.apply(datumClass), ClassTag$.MODULE$.apply(NullWritable.class));

        int recordStreamId = fileStream.getRecordStreamId();
        return pairStream.transform((rdd, validTime) -> rdd.mapPartitions(tuples -> {
            LongAccumulator accumulator = CountedFileInputDStream.getAccumulator(recordStreamId, validTime);
            return new CountAndUpdateAccumulator<>(accumulator, Iterators.transform(tuples, Tuple2::_1));
        }));
    }

    private AvroFileInputDStreamFactory() {
    }

    /**
     * Counts the entries returned by the iterator and reports the results to the given {@link LongAccumulator}.
     * <p>
     * The complexity of this class exists to work around what appears to be a bug in Spark. When the accumulator is
     * used in a multi-partition (multi-task) situation, and is updated for every single record, then the accumulator
     * periodically loses an increment. This class batches up the count and reports to the Spark accumulator just once
     * at the end. Hopefully this won't be needed in the future.
     */
    private static class CountAndUpdateAccumulator<T> implements Iterator<T> {
        private final LongAccumulator accumulator;
        private final Iterator<T> iterator;

        private int unreportedAccumulation = 0;

        CountAndUpdateAccumulator(LongAccumulator accumulator, Iterator<T> iterator) {
            this.accumulator = accumulator;
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if (!result && unreportedAccumulation > 0) {
                accumulator.add(unreportedAccumulation);
                unreportedAccumulation = 0;
            }
            return result;
        }

        @Override
        public T next() {
            unreportedAccumulation += 1;
            return iterator.next();
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }
}
