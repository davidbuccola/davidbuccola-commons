package net.davidbuccola.commons.jersey;

import net.davidbuccola.commons.avro.GenericAvroCodec;
import org.apache.avro.generic.GenericRecord;
import org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * A {@link MessageBodyReader}/{@link MessageBodyWriter} for handling a {@link GenericRecord}.
 */
@Provider
@Produces("application/*")
@Consumes("application/*")
public final class GenericRecordJsonProvider extends AbstractMessageReaderWriterProvider<GenericRecord> {

    private final GenericAvroCodec avroCodec = new GenericAvroCodec();

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return GenericRecord.class.isAssignableFrom(type) && mediaType.getSubtype().endsWith("json");
    }

    @Override
    public void writeTo(GenericRecord record, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) {
        avroCodec.encodeJson(record, entityStream);
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return GenericRecord.class.isAssignableFrom(type);
    }

    @Override
    public GenericRecord readFrom(Class<GenericRecord> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) {
        throw new UnsupportedOperationException("Not implemented yet"); //TODO
    }
}
