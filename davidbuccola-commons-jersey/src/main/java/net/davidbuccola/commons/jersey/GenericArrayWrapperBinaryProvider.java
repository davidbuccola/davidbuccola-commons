package net.davidbuccola.commons.jersey;

import net.davidbuccola.commons.avro.GenericArrayWrapper;
import net.davidbuccola.commons.avro.GenericAvroCodec;
import net.davidbuccola.commons.avro.GenericRecordWrapper;
import net.davidbuccola.commons.avro.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import static net.davidbuccola.commons.jersey.SchemaHeaderUtils.extractSchemaIdFromHttpHeader;
import static net.davidbuccola.commons.jersey.SchemaHeaderUtils.getSchemaIdentifiedInHeader;

/**
 * A {@link MessageBodyReader}/{@link MessageBodyWriter} for handling a {@link GenericRecord}.
 */
@Provider
@Produces("application/*")
@Consumes("application/*")
public final class GenericArrayWrapperBinaryProvider extends AbstractMessageReaderWriterProvider<List<? extends GenericRecordWrapper>> {

    @Inject
    public SchemaRegistry schemaRegistry;

    private final GenericAvroCodec<GenericArray<GenericRecord>> avroCodec = new GenericAvroCodec<>();

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return List.class.isAssignableFrom(type)
            && genericType instanceof ParameterizedType
            && GenericRecordWrapper.class.isAssignableFrom((Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[0])
            && mediaType.getSubtype().endsWith("avro");
    }

    @Override
    public void writeTo(List<? extends GenericRecordWrapper> wrappers, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream inputStream) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return List.class.isAssignableFrom(type)
            && genericType instanceof ParameterizedType
            && GenericRecordWrapper.class.isAssignableFrom((Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[0])
            && mediaType.getSubtype().endsWith("avro")
            && extractSchemaIdFromHttpHeader(mediaType.toString()).isPresent();
    }

    @Override
    public List<? extends GenericRecordWrapper> readFrom(Class<List<? extends GenericRecordWrapper>> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream inputStream) {
        Schema schema = getSchemaIdentifiedInHeader(schemaRegistry, mediaType.toString());
        if (!isProperlyShapedSchema(schema)) {
            throw new NotAcceptableWithReasonException("Schema is not shaped properly for this request. Should be an array of records");
        }

        try {
            GenericArray<GenericRecord> decodedData = avroCodec.decodeBinary(schema, inputStream);
            return new GenericArrayWrapper<>(decodedData, getElementClass(genericType));

        } catch (WebApplicationException e) {
            throw e;

        } catch (Exception e) {
            throw new BadRequestException(e);
        }
    }

    /**
     * Indicates whether the schema is in the ballpark. (It doesn't fully validate).
     */
    private static boolean isProperlyShapedSchema(Schema schema) {
        return schema.getType().equals(Schema.Type.ARRAY) && schema.getElementType().getType().equals(Schema.Type.RECORD);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> getElementClass(Type genericType) {
        return (Class<T>) ((ParameterizedType) genericType).getActualTypeArguments()[0];
    }
}
