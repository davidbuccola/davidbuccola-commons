package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * An {@link AvroCodec} that codes {@link GenericRecord}s to and from binary AVRO.
 */
public class GenericAvroCodec<T extends GenericContainer> extends AvroCodecBase<T> {

    protected final DatumReader<T> newDatumReader(Schema writerSchema, Schema readerSchema) {
        return new GenericDatumReaderWithSelectableStringType<>(writerSchema, readerSchema, GenericData.StringType.String);
    }

    protected final DatumWriter<T> newDatumWriter(Schema writerSchema) {
        return new GenericDatumWriter<>(writerSchema);
    }
}
