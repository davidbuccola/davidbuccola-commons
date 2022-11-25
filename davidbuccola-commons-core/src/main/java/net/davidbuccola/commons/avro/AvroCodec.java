package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static java.util.Collections.singletonList;

/**
 * Encodes and decodes data based on an AVRO {@link Schema}.
 */
public interface AvroCodec<T extends GenericContainer> {

    /**
     * Decodes AVRO binary.
     */
    default T decodeBinary(Schema schema, InputStream encodedData) {
        return decodeBinary(schema, schema, encodedData);
    }

    /**
     * Decodes AVRO binary.
     */
    default T decodeBinary(Schema schema, ByteBuffer encodedData) {
        return decodeBinary(schema, schema, encodedData);
    }

    /**
     * Decodes AVRO binary.
     */
    T decodeBinary(Schema writerSchema, Schema readerSchema, InputStream encodedData);

    /**
     * Decodes AVRO binary.
     */
    default T decodeBinary(Schema writerSchema, Schema readerSchema, ByteBuffer encodedData) {
        return decodeBinary(writerSchema, readerSchema, new ByteBufferInputStream(singletonList(encodedData)));
    }

    /**
     * Decodes JSON which was encoded using an AVRO schema.
     */
    default T decodeJson(Schema schema, InputStream encodedData) {
        return decodeJson(schema, schema, encodedData);
    }

    /**
     * Decodes JSON which was encoded using an AVRO schema.
     */
    default T decodeJson(Schema schema, String encodedData) {
        return decodeJson(schema, schema, encodedData);
    }

    /**
     * Decodes JSON which was encoded using an AVRO schema.
     */
    T decodeJson(Schema writerSchema, Schema readerSchema, InputStream encodedData);

    /**
     * Decodes JSON which was encoded using an AVRO schema.
     */
    T decodeJson(Schema writerSchema, Schema readerSchema, String encodedData);

    /**
     * Encodes a record into AVRO binary.
     */
    ByteBuffer encodeBinary(GenericContainer record);

    /**
     * Encodes a record into AVRO binary.
     */
    void encodeBinary(GenericContainer datum, OutputStream outputStream);

    /**
     * Encodes a record into JSON according to the AVRO schema.
     */
    String encodeJson(GenericContainer record);

    /**
     * Encodes a record into JSON according to the AVRO schema.
     */
    void encodeJson(GenericContainer datum, OutputStream outputStream);

}
