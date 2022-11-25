package net.davidbuccola.commons.jersey;

import net.davidbuccola.commons.avro.SchemaRegistry;
import org.apache.avro.Schema;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities to help with {@link Schema}.
 */
public final class SchemaHeaderUtils {

    private static final Pattern SCHEMA_PARAMETER_PATTERN = Pattern.compile(".+?;schema=([^;]+)");

    private SchemaHeaderUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Looks up the schema identified in an HTTP header. The schema is identified using an HTTP header parameter called
     * "schema".
     * <p>
     * A header with a schema id looks like this: "application/json;schema=fingerprint".
     */
    public static Schema getSchemaIdentifiedInHeader(SchemaRegistry schemaRegistry, String httpHeader) {
        String schemaId = extractSchemaIdFromHttpHeader(httpHeader)
            .orElseThrow(() ->
                new BadRequestWithReasonException("valid 'schema' parameter not found in the Accept header"));

        return schemaRegistry.getSchema(schemaId)
            .orElseThrow(() ->
                new NotAcceptableWithReasonException(
                    "Schema with id '" + schemaId + "' isn't known. Register it with a post to /schemas"));
    }

    /**
     * Extracts a schema id from an HTTP header. The schema id is specified using an HTTP header parameter called
     * "schema".
     * <p>
     * A header with a schema id looks like this: "application/json;schema=fingerprint".
     */
    public static Optional<String> extractSchemaIdFromHttpHeader(String header) {
        Matcher matcher = SCHEMA_PARAMETER_PATTERN.matcher(header);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        } else {
            return Optional.empty();
        }
    }
}
