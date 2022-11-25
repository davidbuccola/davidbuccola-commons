package net.davidbuccola.commons.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry of {@link Schema}.
 * <p>
 * The registry is a transient cache of {@link Schema} used at runtime. Long term storage of {@link Schema} is handled
 * by a {@link SchemaRepository}.
 */
@Singleton
public final class SchemaRegistry {

    private final SchemaRepository repository;
    private final Map<String, Schema> schemasById = new ConcurrentHashMap<>();
    private final Map<Schema, String> idsBySchema = new ConcurrentHashMap<>();

    @Inject
    public SchemaRegistry(SchemaRepository repository) {
        this.repository = repository;
    }

    /**
     * Gets a {@link Schema} by id.
     *
     * @param schemaId the opaque schema id previous obtained through {@link #getSchemaId}.
     */
    public Optional<Schema> getSchema(String schemaId) {
        return Optional.ofNullable(schemasById.computeIfAbsent(schemaId, schemaIdToo ->
            repository.getSchema(schemaId).orElse(null)));
    }

    /**
     * Gets the unique id for a {@link Schema}.
     *
     * @return an opaque schema id that can later be used with {@link #getSchema}.
     */
    public String getSchemaId(Schema schema) {
        return idsBySchema.computeIfAbsent(schema, key -> computeSchemaId(schema));
    }

    /**
     * Registers (and persists if necessary) a {@link Schema}.
     *
     * @return an opaque schema id that can later be used with {@link #getSchema}.
     */
    public String registerSchema(Schema schema) {
        String schemaId = getSchemaId(schema);
        if (getSchema(schemaId).isEmpty()) {
            repository.insertSchema(schemaId, schema);
            schemasById.put(schemaId, schema);
        }
        return schemaId;
    }

    private static String computeSchemaId(Schema schema) {
        try {
            return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(SchemaNormalization.parsingFingerprint("MD5", schema));

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to generate schema id", e);
        }
    }
}
