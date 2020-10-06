package net.davidbuccola.commons;

import org.apache.avro.Schema;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Utilities to help with {@link Schema}.
 */
public class SchemaUtils {

    private SchemaUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Determines the net schema of a field that might be nullable. The net schema is the schema of the field value when
     * the field is not null.
     */
    public static Schema getNetType(Schema fieldSchema) {
        if (fieldSchema.isUnion() && fieldSchema.isNullable()) {
            if (fieldSchema.getTypes().size() != 2) {
                throw new IllegalArgumentException("Can only get the net schema for a union consisting of null and one other type");
            }

            for (Schema innerSchema : fieldSchema.getTypes()) {
                if (innerSchema.isUnion() || !innerSchema.isNullable()) {
                    return innerSchema;
                }
            }
            throw new IllegalArgumentException("Field is not a properly shaped nullable field");

        } else {
            return fieldSchema;
        }
    }

    /**
     * Determines the types that can be placed in a field. For a union, this is the list of all non-null union members.
     * For simple field it is just a singleton list.
     */
    public static List<Schema> getCandidateTypes(Schema fieldSchema) {
        if (fieldSchema.isUnion()) {
            if (fieldSchema.isNullable()) {
                return fieldSchema.getTypes().subList(1, fieldSchema.getTypes().size());
            } else {
                return fieldSchema.getTypes();
            }
        } else {
            return singletonList(fieldSchema);
        }
    }

}
