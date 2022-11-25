package net.davidbuccola.commons.jersey;

import net.davidbuccola.commons.avro.GenericAvroCodec;
import net.davidbuccola.commons.avro.GenericRecordWrapper;
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
public final class GenericRecordWrapperBinaryProvider extends AbstractMessageReaderWriterProvider<GenericRecordWrapper> {

    private final GenericAvroCodec avroCodec = new GenericAvroCodec();

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return GenericRecordWrapper.class.isAssignableFrom(type) && mediaType.getSubtype().endsWith("avro");
    }

    @Override
    public void writeTo(GenericRecordWrapper wrapper, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) {
        avroCodec.encodeBinary(wrapper.getRecord(), entityStream);
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return GenericRecordWrapper.class.isAssignableFrom(type) && mediaType.getSubtype().endsWith("avro");
    }

    @Override
    public GenericRecordWrapper readFrom(Class<GenericRecordWrapper> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) {
        throw new UnsupportedOperationException("Not implemented yet"); //TODO
    }
}
