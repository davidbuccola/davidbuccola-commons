package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

/**
 * An {@link AvroCodec} that codes {@link SpecificRecord}s to and from binary AVRO.
 */
public final class SpecificRecordCodec<T extends SpecificRecord> extends AvroCodecBase<T> {

    @Override
    protected final DatumReader<T> newDatumReader(Schema writerSchema, Schema readerSchema) {
        return new SpecificDatumReader<>(writerSchema, readerSchema);
    }

    @Override
    protected final DatumWriter<T> newDatumWriter(Schema schema) {
        return new SpecificDatumWriter<>(schema);
    }
}
