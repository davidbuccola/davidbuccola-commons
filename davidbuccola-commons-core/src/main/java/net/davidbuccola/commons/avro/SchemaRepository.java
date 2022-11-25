package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;

import java.util.Optional;

/**
 * A persistent store of {@link Schema} indexed by a schema id. For purposes of storage the schema id is an opaque
 * value. The client of the repository owns the semantics (if any) of the id.
 */
public interface SchemaRepository {

    /**
     * Gets a {@link Schema} by id.
     */
    Optional<Schema> getSchema(String schemaId);

    /**
     * Adds a {@link Schema}.
     */
    void insertSchema(String schemaId, Schema schema);

    /**
     * Removes a {@link Schema}.
     */
    void deleteSchema(String schemaId);

}
