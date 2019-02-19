package net.davidbuccola.commons.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.FileInputDStream;
import org.apache.spark.streaming.scheduler.InputInfoTracker;
import org.apache.spark.streaming.scheduler.StreamInputInfo;
import org.apache.spark.util.LongAccumulator;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.nio.file.Path;

import static scala.collection.JavaConversions.mapAsJavaMap;
import static scala.compat.java8.JFunction.func;

/**
 * A variation of {@link FileInputDStream} which exposes a {@link LongAccumulator} for counting the number of records.
 * Normally a {@link FileInputDStream} just reports a count of zero.
 * <p>
 * A special {@link StreamInputInfo} is registered which exposes the accumulated record count for proper reporting in
 * the Spark APIs, Listeners and UI.
 */
class CountedFileInputDStream<T> extends FileInputDStream<T, NullWritable, InputFormat<T, NullWritable>> {

    private final int recordStreamId;

    CountedFileInputDStream(StreamingContext ssc, Path directory, Class<T> recordClass, Class<? extends InputFormat<T, NullWritable>> formatClass, boolean newFilesOnly, Option<Configuration> configuration) {
        super(ssc,
            directory.toAbsolutePath().toString(),
            func(FileInputDStream::defaultFilter),
            newFilesOnly,
            configuration,
            ClassTag$.MODULE$.apply(recordClass),
            ClassTag$.MODULE$.apply(NullWritable.class),
            ClassTag$.MODULE$.apply(formatClass));

        recordStreamId = ssc.getNewInputStreamId();
    }

    @Override
    public Option<RDD<Tuple2<T, NullWritable>>> compute(Time validTime) {
        registerFutureInputStreamInfo(validTime);

        return super.compute(validTime);
    }

    int getRecordStreamId() {
        return recordStreamId;
    }

    static LongAccumulator getAccumulator(int datumStreamId, Time validTime) {
        StreamInputInfo streamingInputInfo = mapAsJavaMap(getInputInfoTracker().getInfo(validTime)).get(datumStreamId);
        return ((FutureStreamInputInfo) streamingInputInfo).getAccumulator();
    }

    private void registerFutureInputStreamInfo(Time validTime) {
        getInputInfoTracker().reportInfo(validTime, new FutureStreamInputInfo(recordStreamId, new LongAccumulator()));
    }

    private static InputInfoTracker getInputInfoTracker() {
        return StreamingContext.getActive().get().scheduler().inputInfoTracker();
    }

    /**
     * A variation of {@link StreamInputInfo} which presents a record count that is finalized at a future because the
     * exact count is not yet known.
     */
    private static class FutureStreamInputInfo extends StreamInputInfo {
        private final LongAccumulator accumulator;

        FutureStreamInputInfo(int streamId, LongAccumulator accumulator) {
            super(streamId, 0, null);

            this.accumulator = accumulator;
        }

        public long numRecords() {
            return accumulator.sum();
        }

        LongAccumulator getAccumulator() {
            return accumulator;
        }
    }
}
