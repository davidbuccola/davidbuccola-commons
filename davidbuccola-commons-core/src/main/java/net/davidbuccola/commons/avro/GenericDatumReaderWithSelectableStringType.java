/*
 * Copyright 2022 salesforce.com, inc.
 * All Rights Reserved
 * Company Confidential
 */

package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericDatumReader;

/**
 * An extension of {@link GenericDatumReader} that allows the Java {@link StringType} to be programmatically chosen
 * instead of using the value specified in the schema.
 */
public class GenericDatumReaderWithSelectableStringType<D> extends GenericDatumReader<D> {

    private final Class<?> stringClass;

    public GenericDatumReaderWithSelectableStringType(StringType stringType) {
        super();
        this.stringClass = chooseStringClass(stringType);
    }

    public GenericDatumReaderWithSelectableStringType(Schema schema, StringType stringType) {
        super(schema);
        this.stringClass = chooseStringClass(stringType);
    }

    public GenericDatumReaderWithSelectableStringType(Schema writer, Schema reader, StringType stringType) {
        super(writer, reader);
        this.stringClass = chooseStringClass(stringType);
    }

    public GenericDatumReaderWithSelectableStringType(Schema writer, Schema reader, GenericData data, StringType stringType) {
        super(writer, reader, data);
        this.stringClass = chooseStringClass(stringType);
    }

    @Override
    protected Class<?> findStringClass(Schema schema) {
        return stringClass;
    }

    private static Class<?> chooseStringClass(StringType stringType) {
        if (stringType == StringType.String) {
            return String.class;
        }
        return CharSequence.class;
    }
}
