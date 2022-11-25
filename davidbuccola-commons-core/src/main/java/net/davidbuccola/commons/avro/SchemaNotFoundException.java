package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;

/**
 * Indicates a {@link Schema} identified by its id could not be found.
 */
public final class SchemaNotFoundException extends RuntimeException {

    public SchemaNotFoundException(String schemaId) {
        super("Can't find schema for id '" + schemaId + "'");
    }
}
