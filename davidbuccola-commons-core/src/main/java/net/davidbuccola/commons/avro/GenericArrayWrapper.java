package net.davidbuccola.commons.avro;

import com.google.common.collect.ForwardingList;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Constructor;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;

public class GenericArrayWrapper<T extends GenericRecordWrapper> extends ForwardingList<T> {

    private final GenericArray<GenericRecord> records;
    private final WrapperFactory<T> wrapperFactory;

    private List<T> wrappedRecords;

    public GenericArrayWrapper(GenericArray<GenericRecord> records, Class<T> elementWrapperClass) {
        this.records = records;
        this.wrapperFactory = new WrapperFactory<>(elementWrapperClass);
    }

    public final GenericArray<GenericRecord> getRecords() {
        return records;
    }

    @Override
    protected List<T> delegate() {
        if (wrappedRecords == null) {
            wrappedRecords = records.stream().map(wrapperFactory::wrap).collect(toUnmodifiableList());
        }
        return wrappedRecords;
    }

    private static class WrapperFactory<T extends GenericRecordWrapper> {

        final Constructor<T> constructor;

        WrapperFactory(Class<T> wrapperClass) {
            try {
                constructor = wrapperClass.getConstructor(GenericRecord.class);

            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Missing " + wrapperClass.getSimpleName() + "(GenericRecord record) constructor");
            }
        }

        T wrap(GenericRecord record) {
            try {
                return constructor.newInstance(record);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
