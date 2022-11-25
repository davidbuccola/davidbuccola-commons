package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A base for concrete {@link AvroCodec} implementations.
 * <p>
 * TODO Optimize
 */
abstract class AvroCodecBase<T extends GenericContainer> implements AvroCodec<T> {

    @Override
    public final T decodeBinary(Schema writerSchema, Schema readerSchema, InputStream encodedData) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(encodedData, null);
            return getDatumReader(writerSchema, readerSchema).read(null, decoder);

        } catch (Exception e) {
            throw new RuntimeException("AVRO decoding failed", e);
        }
    }

    @Override
    public final T decodeJson(Schema writerSchema, Schema readerSchema, InputStream encodedData) {
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, encodedData);
            return getDatumReader(writerSchema, readerSchema).read(null, decoder);

        } catch (Exception e) {
            throw new RuntimeException("AVRO decoding failed", e);
        }
    }

    @Override
    public final T decodeJson(Schema writerSchema, Schema readerSchema, String encodedData) {
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, encodedData);
            return getDatumReader(writerSchema, readerSchema).read(null, decoder);

        } catch (Exception e) {
            throw new RuntimeException("AVRO decoding failed", e);
        }
    }

    @Override
    public final ByteBuffer encodeBinary(GenericContainer datum) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        encodeBinary(datum, outputStream);
        return ByteBuffer.wrap(outputStream.toByteArray(), 0, outputStream.size());
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void encodeBinary(GenericContainer datum, OutputStream outputStream) {
        try {
            Schema schema = datum.getSchema();
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            getDatumWriter(schema).write((T) datum, encoder);
            encoder.flush();

        } catch (Exception e) {
            throw new RuntimeException("AVRO binary encoding failed", e);
        }
    }

    @Override
    public String encodeJson(GenericContainer datum) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        encodeJson(datum, outputStream);

        return outputStream.toString(StandardCharsets.UTF_8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void encodeJson(GenericContainer datum, OutputStream outputStream) {
        try {
            Schema schema = datum.getSchema();
            Encoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            getDatumWriter(schema).write((T) datum, encoder);
            encoder.flush();

        } catch (Exception e) {
            throw new RuntimeException("AVRO JSON encoding failed", e);
        }
    }

    protected abstract DatumReader<T> newDatumReader(Schema writerSchema, Schema readerSchema);

    protected abstract DatumWriter<T> newDatumWriter(Schema writerSchema);

    private DatumReader<T> getDatumReader(Schema writerSchema, Schema readerSchema) {
        //TODO No caching for now. Optimize? Does caching make any difference?
        return newDatumReader(writerSchema, readerSchema);
    }

    private DatumWriter<T> getDatumWriter(Schema schema) {
        //TODO No caching for now. Optimize? Does caching make any difference?
        return newDatumWriter(schema);
    }
}
