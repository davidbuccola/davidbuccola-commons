package net.davidbuccola.commons.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Optional;

public class GenericRecordWrapper {

    private final GenericRecord record;

    public GenericRecordWrapper(GenericRecord record) {
        this.record = record;
    }

    public final GenericRecord getRecord() {
        return record;
    }

    public final String getType() {
        return record.getSchema().getName();
    }

    public final <T> Optional<T> getField(String name, Class<T> valueClass) {
        T value = valueClass.cast(record.get(name));
        if (value == null && record.getSchema().getField(name) == null) {
            throw new IllegalArgumentException("Record has no field named '" + name + "'");
        }
        return Optional.ofNullable(value);
    }

    @SuppressWarnings("unchecked")
    public final <T> List<T> getListField(String name, Class<T> valueClass) {
        List<T> value = (List<T>) record.get(name);
        if (value == null && record.getSchema().getField(name) == null) {
            throw new IllegalArgumentException("Record has no field named '" + name + "'");
        }
        return value;
    }

    public final <T> T getRequiredField(String name, Class<T> valueClass) {
        return getField(name, valueClass).orElseThrow(() ->
            new AvroRuntimeException("Missing required field: " + record.getSchema().getField(name)));
    }

    public final <T> List<T> getRequiredListField(String name, Class<T> valueClass) {
        List<T> value = getListField(name, valueClass);
        if (value == null) {
            throw new AvroRuntimeException("Missing required field: " + record.getSchema().getField(name));
        }
        return value;
    }

    public final <T> void setField(String name, T value) {
        if (record.getSchema().getField(name) == null) {
            throw new IllegalArgumentException("Record has no field named '" + name + "'");
        }
        record.put(name, value);
    }

}
