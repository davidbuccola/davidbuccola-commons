package net.davidbuccola.commons.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.FileInputDStream;
import org.apache.spark.streaming.scheduler.InputInfoTracker;
import org.apache.spark.streaming.scheduler.StreamInputInfo;
import scala.Function1;
import scala.Option;
import scala.reflect.ClassTag;

import static scala.compat.java8.JFunction.func;

/**
 * Allows extensions of {@link FileInputDStream}. This is used, for example, to properly report the record count of a
 * {@link FileInputDStream}.
 * <p>
 * This class is intentionally written in Java to "hack" Spark. Java is able to access package-private classes that are
 * needed to do the job.
 */
public abstract class FileInputDStreamExtensionHack<K, V, F extends InputFormat<K, V>> extends FileInputDStream<K, V, F> {

    protected FileInputDStreamExtensionHack(StreamingContext ssc, String directory, Function1<org.apache.hadoop.fs.Path, scala.Boolean> filter, boolean newFilesOnly, Option<Configuration> configuration, ClassTag<K> keyTag, ClassTag<V> valueTag, ClassTag<F> inputFormatTag) {
        super(ssc, directory, func(filter::apply), newFilesOnly, configuration, keyTag, valueTag, inputFormatTag);
    }

    protected final void reportInputInfo(StreamInputInfo inputInfo, Time validTime) {
        getInputInfoTracker().reportInfo(validTime, inputInfo);
    }

    protected final int getNewInputStreamId() {
        return ssc().getNewInputStreamId();
    }

    private static InputInfoTracker getInputInfoTracker() {
        return StreamingContext.getActive().get().scheduler().inputInfoTracker();
    }
}
