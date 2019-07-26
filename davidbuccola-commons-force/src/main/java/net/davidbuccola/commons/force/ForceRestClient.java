package net.davidbuccola.commons.force;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.salesforce.streaming.core.BasicHttpClientFactory;
import com.salesforce.streaming.core.HttpClientFactory;
import com.salesforce.streaming.core.auth.Authentication;
import com.salesforce.streaming.core.auth.Authenticator;
import com.salesforce.streaming.core.auth.BasicAuthenticator;
import com.salesforce.streaming.core.auth.BasicIdentity;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.StatusCode;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpRequestException;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Lists.partition;
import static com.salesforce.streaming.core.util.Slf4jUtils.debug;
import static com.salesforce.streaming.core.util.Slf4jUtils.error;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static net.davidbuccola.commons.FutureUtils.forAllOf;


@SuppressWarnings("WeakerAccess")
public final class ForceRestClient {

    private static final String REST_API_VERSION = "47.0";
    private static final int DEFAULT_CONCURRENCY = 1;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ForceRestClient.class);

    private final String server;
    private final String username;
    private final String password;
    private final Authenticator authenticator;
    private final HttpClient httpClient;
    private final Semaphore rightToPerformSObjectOperation;

    private Authentication authentication;

    public ForceRestClient(CommandLine command) {
        this(
            command.getOptionValue("server", "http://localhost:6109"),
            command.getOptionValue("username"),
            command.getOptionValue("password"),
            parseInt(command.getOptionValue("concurrency", Integer.toString(DEFAULT_CONCURRENCY))));
    }

    public ForceRestClient(String server, String username, String password, int concurrency) {
        this.server = server;
        this.username = username;
        this.password = password;

        rightToPerformSObjectOperation = new Semaphore(concurrency);

        HttpClientFactory httpClientFactory = new BasicHttpClientFactory();
        httpClient = httpClientFactory.getHttpClient();
        authenticator = new BasicAuthenticator()
            .withApiVersion(REST_API_VERSION)
            .withHttpClientFactory(httpClientFactory)
            .withServerUrl(server);
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("server").hasArg().argName("url").desc("Core login URL").build());
        options.addOption(Option.builder().longOpt("username").hasArg().argName("username").desc("Username").required().build());
        options.addOption(Option.builder().longOpt("password").hasArg().argName("password").desc("Password").required().build());
        options.addOption(Option.builder().longOpt("concurrency").hasArg().argName("number").desc("Maximum number of concurrent sobject operations").build());

        return options;
    }

    public Authentication getAuthentication() {
        if (authentication == null) {
            authentication = authenticator.authenticate(new BasicIdentity(username, password));

            debug(log, "Authenticated", () -> ImmutableMap.of("server", server, "username", username));
        }
        return authentication;
    }

    public void query(String soql, Consumer<QueryResult> resultProcessor) throws ExecutionException, InterruptedException {
        QueryResult result = null;
        do {
            if (result == null) {
                result = query(soql).get();
            } else {
                result = queryMore(result.getQueryLocator()).get();
            }
            resultProcessor.accept(result);
        } while (!result.isDone());

    }

    public CompletableFuture<QueryResult> query(String soql) {
        CompletableFuture<QueryResult> futureResult = new CompletableFuture<>();
        Request request = httpClient.newRequest(buildQueryURI(getAuthentication(), soql))
            .accept("application/json")
            .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken());

        long beginMillis = System.currentTimeMillis();
        request.send(new BufferingResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    if (HttpStatus.isSuccess(response.getStatus())) {
                        QueryResult result = toQueryResult(parseJson(getContentAsString()));

                        debug(log, "Query succeeded", () -> ImmutableMap.of(
                            "soql", soql,
                            "elapsedMillis", System.currentTimeMillis() - beginMillis));

                        futureResult.complete(result);
                    } else {
                        throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                    }
                } catch (Exception e) {
                    log.debug("Query failed", e);

                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
                log.debug("Query failed", failure);

                futureResult.completeExceptionally(failure);
            }

            @Override
            public void onComplete(Result result) {
                // The necessary work is done in "onSuccess" and "onFailure".
            }
        });
        return futureResult;
    }

    public CompletableFuture<QueryResult> queryMore(String queryLocator) {
        CompletableFuture<QueryResult> futureResult = new CompletableFuture<>();
        Request request = httpClient.newRequest(buildQueryMoreURI(getAuthentication(), queryLocator))
            .accept("application/json")
            .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken());

        long beginMillis = System.currentTimeMillis();
        request.send(new BufferingResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    if (HttpStatus.isSuccess(response.getStatus())) {
                        QueryResult result = toQueryResult(parseJson(getContentAsString()));

                        debug(log, "Query succeeded", () -> ImmutableMap.of(
                            "queryLocator", queryLocator,
                            "elapsedMillis", System.currentTimeMillis() - beginMillis));

                        futureResult.complete(result);
                    } else {
                        throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                    }
                } catch (Exception e) {
                    log.debug("Query failed", e);

                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
                log.debug("Query failed", failure);

                futureResult.completeExceptionally(failure);
            }

            @Override
            public void onComplete(Result result) {
                // The necessary work is done in "onSuccess" and "onFailure".
            }
        });
        return futureResult;
    }

    public CompletableFuture<List<UpsertResult>> upsert(List<SObject> records, int batchSize) {
        if (records.size() > batchSize) {
            return forAllOf(partition(records, batchSize), false, batch -> upsert(batch, batchSize)) // Create in batches
                .thenApply(batchResults ->
                    batchResults.stream().flatMap(Collection::stream).collect(toList())); // Combine results
        }

        rightToPerformSObjectOperation.acquireUninterruptibly();
        try {
            CompletableFuture<List<UpsertResult>> futureResult = new CompletableFuture<>();
            Request request = httpClient.POST(buildCompositeSObjectsURI(getAuthentication()))
                .accept("application/json")
                .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken())
                .content(new StringContentProvider("application/json", buildCompositeSObjectsBody(records), Charsets.UTF_8));

            long beginMillis = System.currentTimeMillis();
            request.send(new BufferingResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        if (HttpStatus.isSuccess(response.getStatus())) {
                            AtomicInteger numberOfSuccesses = new AtomicInteger(0);
                            List<UpsertResult> results = toUpsertResults(parseJson(getContentAsString()));
                            Multimap<String, UpsertResult> resultsByErrorMessage = HashMultimap.create();
                            for (int i = 0, limit = records.size(); i < limit; i++) {
                                UpsertResult result = results.get(i);
                                if (result.isSuccess()) {
                                    numberOfSuccesses.incrementAndGet();
                                    if (result.isCreated()) {
                                        records.get(i).setId(result.getId());
                                    }
                                } else {
                                    StringBuilder errorMessage = new StringBuilder();
                                    for (IError error : result.getErrors()) {
                                        errorMessage.append(String.format(
                                            "%n  message=%s, statusCode=%s, fields=%s",
                                            error.getMessage(), error.getStatusCode(), Arrays.toString(error.getFields())));
                                    }
                                    resultsByErrorMessage.put(errorMessage.toString(), result);
                                }
                            }
                            if (numberOfSuccesses.get() > 0) {
                                debug(log, "Upserted records", () -> ImmutableMap.of(
                                    "count", numberOfSuccesses,
                                    "elapsedMillis", System.currentTimeMillis() - beginMillis));
                            }

                            if (resultsByErrorMessage.size() > 0) {
                                resultsByErrorMessage.asMap().forEach((message, resultsWithSameMessage) ->
                                    error(log, "Failed to upsert records", null, () -> ImmutableMap.of(
                                        "count", resultsWithSameMessage.size(),
                                        "message", message)));

                                throw new RuntimeException(String.join("\n", resultsByErrorMessage.keySet()));
                            }
                            rightToPerformSObjectOperation.release();
                            futureResult.complete(results);
                        } else {
                            throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                        }
                    } catch (Exception e) {
                        log.debug("Upsert failed", e);

                        rightToPerformSObjectOperation.release();
                        futureResult.completeExceptionally(e);
                    }
                }

                @Override
                public void onFailure(Response response, Throwable failure) {
                    log.debug("Create failed", failure);

                    rightToPerformSObjectOperation.release();
                    futureResult.completeExceptionally(failure);
                }

                @Override
                public void onComplete(Result result) {
                    // The necessary work is done in "onSuccess" and "onFailure".
                }
            });
            return futureResult;

        } catch (RuntimeException e) {
            rightToPerformSObjectOperation.release();
            throw e;
        }
    }

    private static URI buildQueryURI(Authentication authentication, String soql) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/data/v" + authentication.getApiVersion() + "/query/",
                "q=" + soql,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen");
        }
    }

    private static URI buildQueryMoreURI(Authentication authentication, String queryLocator) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/data/v" + authentication.getApiVersion() + "/query/" + queryLocator,
                null,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen");
        }
    }

    private URI buildCompositeSObjectsURI(Authentication authentication) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/data/v" + authentication.getApiVersion() + "/composite/sobjects/",
                null,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen");
        }
    }

    private static String buildCompositeSObjectsBody(List<SObject> sObjects) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        body.put("allOrNone", true);
        body.set("records", toJsonObjects(sObjects));
        return body.toString();
    }

    private static JsonNode parseJson(String content) throws IOException {
        return objectMapper.readTree(nullToEmpty(content));
    }

    private static ArrayNode toJsonObjects(List<SObject> sObjects) {
        ArrayNode node = JsonNodeFactory.instance.arrayNode();
        for (SObject record : sObjects) {
            node.add(toJsonObject(record));
        }
        return node;
    }

    @SuppressWarnings("unchecked")
    private static ObjectNode toJsonObject(SObject sObject) {
        ObjectNode jsonObject = JsonNodeFactory.instance.objectNode();
        jsonObject.set("attributes", JsonNodeFactory.instance.objectNode().put("type", sObject.getType()));
        sObject.getChildren().forEachRemaining(field -> {
            String fieldName = field.getName().getLocalPart();
            if (!fieldName.equals("type")) {
                Object value = sObject.getSObjectField(fieldName);
                if (value != null) {
                    Class<?> valueClass = value.getClass();
                    if (valueClass == Integer.class) {
                        jsonObject.put(fieldName, (Integer) value);
                    } else if (valueClass == Long.class) {
                        jsonObject.put(fieldName, (Long) value);
                    } else if (valueClass == String.class) {
                        jsonObject.put(fieldName, (String) value);
                    } else if (valueClass == Double.class) {
                        jsonObject.put(fieldName, (Double) value);
                    } else if (valueClass == Boolean.class) {
                        jsonObject.put(fieldName, (Boolean) value);
                    } else if (valueClass == Date.class) {
                        jsonObject.put(fieldName, ((Date) value).toInstant().toString());
                    } else if (valueClass == SObject.class) {
                        jsonObject.set(fieldName, toJsonObject((SObject) value));
                    } else if (List.class.isAssignableFrom(valueClass)) {
                        jsonObject.set(fieldName, toJsonObjects((List<SObject>) value));
                    } else {
                        throw new IllegalArgumentException("Unsupported SObject field data value");
                    }
                }
            }
        });
        return jsonObject;
    }

    private static QueryResult toQueryResult(JsonNode resultNode) {
        QueryResult result = new QueryResult();
        result.setDone(resultNode.get("done").asBoolean());
        result.setSize(resultNode.get("totalSize").asInt());
        if (resultNode.has("nextRecordsUrl")) {
            String nextRecordsUrl = resultNode.get("nextRecordsUrl").asText();
            String queryLocator = nextRecordsUrl.substring(nextRecordsUrl.lastIndexOf("/") + 1);
            result.setQueryLocator(queryLocator);
        }
        result.setRecords(toSObjects(resultNode.get("records")).toArray(new SObject[0]));
        return result;
    }

    private static List<SObject> toSObjects(JsonNode jsonObjects) {
        List<SObject> sObjects = new ArrayList<>();
        for (JsonNode sObject : jsonObjects) {
            sObjects.add(toSObject((ObjectNode) sObject));
        }
        return sObjects;
    }

    private static SObject toSObject(ObjectNode jsonObject) {
        SObject sObject = new SObject(jsonObject.get("attributes").get("type").asText());
        jsonObject.fields().forEachRemaining(fieldEntry -> {
            String fieldName = fieldEntry.getKey();
            if (!fieldName.equals("attributes")) {
                JsonNode fieldNode = fieldEntry.getValue();
                switch (fieldNode.getNodeType()) {
                    case ARRAY:
                    case MISSING:
                    case BINARY:
                    case POJO:
                        throw new IllegalArgumentException("Unsupported data type");

                    case BOOLEAN:
                        sObject.setSObjectField(fieldName, fieldNode.booleanValue());
                        break;

                    case STRING:
                        sObject.setSObjectField(fieldName, fieldNode.textValue());
                        break;

                    case NULL:
                        break;

                    case NUMBER:
                        sObject.setSObjectField(fieldName, fieldNode.intValue());
                        break;

                    case OBJECT:
                        sObject.setSObjectField(fieldName, toSObject((ObjectNode) fieldNode));
                        break;
                }
            }
        });
        return sObject;
    }

    private static List<UpsertResult> toUpsertResults(JsonNode resultsNode) {
        List<UpsertResult> results = new ArrayList<>();
        for (JsonNode resultNode : resultsNode) {
            results.add(toUpsertResult(resultNode));
        }
        return results;
    }

    private static UpsertResult toUpsertResult(JsonNode resultNode) {
        UpsertResult result = new UpsertResult();
        result.setSuccess(resultNode.has("success") && resultNode.get("success").asBoolean());
        result.setCreated(result.isSuccess());
        if (resultNode.has("id")) {
            result.setId(resultNode.get("id").asText());
        }
        if (resultNode.has("errors")) {
            result.setErrors(toErrors(resultNode.get("errors")).toArray(new IError[0]));
        }
        return result;
    }

    private static List<IError> toErrors(JsonNode errorsNode) {
        List<IError> errors = new ArrayList<>();
        for (JsonNode errorNode : errorsNode) {
            errors.add(toError(errorNode));
        }
        return errors;
    }

    private static IError toError(JsonNode errorNode) {
        IError error = new com.sforce.soap.partner.Error();
        error.setStatusCode(StatusCode.valueOf(errorNode.get("statusCode").asText()));
        error.setMessage(errorNode.get("message").asText());
        if (errorNode.has("fields")) {
            error.setFields(stream(errorNode.get("fields").spliterator(), false).map(JsonNode::asText).toArray(String[]::new));
        }
        return error;
    }

    private static String extractErrorMessage(String content) {
        try {
            JsonNode firstErrorNode = parseJson(emptyToNull(content)).get(0);
            if (firstErrorNode.has("message")) {
                return firstErrorNode.get("message").asText();

            } else if (firstErrorNode.has("errorCode")) {
                return firstErrorNode.get("errorCode").asText();
            }

        } catch (Exception e) {
            // For parsing errors fall through and return a default message
        }
        return emptyToNull(content);
    }
}
