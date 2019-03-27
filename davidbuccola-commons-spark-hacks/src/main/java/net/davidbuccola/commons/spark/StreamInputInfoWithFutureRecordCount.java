package net.davidbuccola.commons.spark;

import org.apache.spark.streaming.scheduler.StreamInputInfo;
import scala.Function0;

/**
 * A variation of {@link StreamInputInfo} which presents a record count that is finalized at a future time because the
 * exact count is not yet known.
 * <p>
 * This class is intentionally written in Java to allow the record count to be supplied lazily (the Scala compiler
 * prevents it).
 */
public class StreamInputInfoWithFutureRecordCount extends StreamInputInfo {
    private final Function0<Long> recordCountSupplier;

    public StreamInputInfoWithFutureRecordCount(int streamId, Function0<Long> recordCountSupplier) {
        super(streamId, 0, null);

        this.recordCountSupplier = recordCountSupplier;
    }

    public long numRecords() {
        return recordCountSupplier.apply();
    }
}
