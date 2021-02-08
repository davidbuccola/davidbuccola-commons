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
import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.SObject;
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
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static net.davidbuccola.commons.FutureUtils.forAllOf;


@SuppressWarnings({"WeakerAccess", "unused"})
public final class ForceRestClient {

    public static final String DEFAULT_API_VERSION = "52.0";
    public static final int DEFAULT_CONCURRENCY = 1;
    public static final int DEFAULT_BATCH_SIZE = 200;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ForceRestClient.class);

    private final String server;
    private final String username;
    private final String password;
    private final Authenticator authenticator;
    private final HttpClient httpClient;
    private final Semaphore rightToPerformSObjectOperation;

    private Authentication authentication;

    public ForceRestClient(String server, String username, String password) {
        this(server, username, password, DEFAULT_API_VERSION, DEFAULT_CONCURRENCY);
    }

    public ForceRestClient(String server, String username, String password, HttpClientFactory httpClientFactory) {
        this(server, username, password, DEFAULT_API_VERSION, DEFAULT_CONCURRENCY, httpClientFactory);
    }

    public ForceRestClient(String server, String username, String password, String apiVersion) {
        this(server, username, password, apiVersion, DEFAULT_CONCURRENCY);
    }

    public ForceRestClient(String server, String username, String password, String apiVersion, HttpClientFactory httpClientFactory) {
        this(server, username, password, apiVersion, DEFAULT_CONCURRENCY, httpClientFactory);
    }

    public ForceRestClient(String server, String username, String password, int concurrency) {
        this(server, username, password, DEFAULT_API_VERSION, concurrency);
    }

    public ForceRestClient(String server, String username, String password, int concurrency, HttpClientFactory httpClientFactory) {
        this(server, username, password, DEFAULT_API_VERSION, concurrency, httpClientFactory);
    }

    public ForceRestClient(String server, String username, String password, String apiVersion, int concurrency) {
        this(server, username, password, apiVersion, concurrency, new BasicHttpClientFactory());
    }

    public ForceRestClient(String server, String username, String password, String apiVersion, int concurrency, HttpClientFactory httpClientFactory) {
        this.server = server;
        this.username = username;
        this.password = password;

        rightToPerformSObjectOperation = new Semaphore(concurrency);

        httpClient = httpClientFactory.getHttpClient();
        authenticator = new BasicAuthenticator()
            .withApiVersion(apiVersion)
            .withHttpClientFactory(httpClientFactory)
            .withServerUrl(server);
    }

    public HttpClient getHttpClient() {
        return httpClient;
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
                        QueryResult result = buildQueryResult(parseJson(getContentAsString()));

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
                        QueryResult result = buildQueryResult(parseJson(getContentAsString()));

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

    public CompletableFuture<List<UpsertResult>> insert(List<SObject> records) {
        return insert(records, DEFAULT_BATCH_SIZE);
    }

    public CompletableFuture<List<UpsertResult>> insert(List<SObject> records, int batchSize) {
        return upsert(records, batchSize, "POST");
    }

    public CompletableFuture<List<UpsertResult>> update(List<SObject> records) {
        return update(records, DEFAULT_BATCH_SIZE);
    }

    public CompletableFuture<List<UpsertResult>> update(List<SObject> records, int batchSize) {
        return upsert(records, batchSize, "PATCH");
    }

    private CompletableFuture<List<UpsertResult>> upsert(List<SObject> records, int batchSize, String method) {
        if (records.size() > batchSize) {
            return forAllOf(partition(records, batchSize), false, batch -> insert(batch, batchSize)) // Create in batches
                .thenApply(batchResults ->
                    batchResults.stream().flatMap(Collection::stream).collect(toList())); // Combine results
        }

        rightToPerformSObjectOperation.acquireUninterruptibly();
        try {
            CompletableFuture<List<UpsertResult>> futureResult = new CompletableFuture<>();
            Request request = httpClient.newRequest(buildCompositeSObjectsURI(getAuthentication())).method(method)
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
                            List<UpsertResult> results = buildUpsertResults(parseJson(getContentAsString()));
                            Multimap<String, UpsertResult> resultsByErrorMessage = HashMultimap.create();
                            for (int i = 0, limit = records.size(); i < limit; i++) {
                                UpsertResult result = results.get(i);
                                if (result.isSuccess()) {
                                    numberOfSuccesses.incrementAndGet();
                                    if (result.isCreated()) {
                                        records.get(i).setId(result.getId());
                                    }
                                } else {
                                    StringBuilder message = new StringBuilder();
                                    for (IError error : result.getErrors()) {
                                        if (message.length() > 0) {
                                            message.append("\n");
                                        }
                                        message.append(String.format(
                                            "%s, statusCode=%s, fields=%s",
                                            error.getMessage(), error.getStatusCode(), Arrays.toString(error.getFields())));
                                    }
                                    resultsByErrorMessage.put(message.toString(), result);
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
                    log.debug("Upsert failed before getting started", failure);

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

    public CompletableFuture<DescribeSObjectResult> describeSObject(String entityName) {
        CompletableFuture<DescribeSObjectResult> futureResult = new CompletableFuture<>();
        Request request = httpClient.newRequest(buildDescribeURI(getAuthentication(), entityName))
            .accept("application/json")
            .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken());

        long beginMillis = System.currentTimeMillis();
        request.send(new BufferingResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    if (HttpStatus.isSuccess(response.getStatus())) {
                        DescribeSObjectResult result = buildDescribeSObjectResult(parseJson(getContentAsString()));

                        debug(log, "Describe succeeded", () -> ImmutableMap.of(
                            "entityName", entityName,
                            "elapsedMillis", System.currentTimeMillis() - beginMillis));

                        futureResult.complete(result);
                    } else {
                        throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                    }
                } catch (Exception e) {
                    log.debug("Describe failed", e);

                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
                log.debug("Describe failed", failure);

                futureResult.completeExceptionally(failure);
            }

            @Override
            public void onComplete(Result result) {
                // The necessary work is done in "onSuccess" and "onFailure".
            }
        });
        return futureResult;
    }

    public CompletableFuture<Void> setPassword(String userId, String password) {
        CompletableFuture<Void> futureResult = new CompletableFuture<>();
        Request request = httpClient.POST(buildSetPasswordURI(getAuthentication(), userId))
            .accept("application/json")
            .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken())
            .content(new StringContentProvider("application/json", buildSetPasswordBody(password), Charsets.UTF_8));

        long beginMillis = System.currentTimeMillis();
        request.send(new BufferingResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    if (HttpStatus.isSuccess(response.getStatus())) {

                        debug(log, "Set password succeeded", () -> ImmutableMap.of(
                            "userId", userId,
                            "elapsedMillis", System.currentTimeMillis() - beginMillis));

                        futureResult.complete(null);
                    } else {
                        throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                    }
                } catch (Exception e) {
                    log.debug("Set password failed", e);

                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
                log.debug("Set password failed", failure);

                futureResult.completeExceptionally(failure);
            }

            @Override
            public void onComplete(Result result) {
                // The necessary work is done in "onSuccess" and "onFailure".
            }
        });
        return futureResult;
    }

    public CompletableFuture<GetUserInfoResult> getUserInfo() {
        CompletableFuture<GetUserInfoResult> futureResult = new CompletableFuture<>();
        Request request = httpClient.newRequest(buildGetUserInfoURI(getAuthentication()))
            .accept("application/json")
            .header(HttpHeader.AUTHORIZATION, "Bearer " + authentication.getBearerToken());

        long beginMillis = System.currentTimeMillis();
        request.send(new BufferingResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    if (HttpStatus.isSuccess(response.getStatus())) {
                        GetUserInfoResult result = buildGetUserInfoResult(parseJson(getContentAsString()));

                        debug(log, "Get user info succeeded", () -> ImmutableMap.of(
                            "elapsedMillis", System.currentTimeMillis() - beginMillis));

                        futureResult.complete(result);
                    } else {
                        throw new HttpRequestException(response.getReason() + ": " + extractErrorMessage(getContentAsString()), request);
                    }
                } catch (Exception e) {
                    log.debug("Get user info failed", e);

                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
                log.debug("Get user info failed", failure);

                futureResult.completeExceptionally(failure);
            }

            @Override
            public void onComplete(Result result) {
                // The necessary work is done in "onSuccess" and "onFailure".
            }
        });
        return futureResult;
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
            throw new IllegalStateException("This should never happen", e);
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
            throw new IllegalStateException("This should never happen", e);
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
            throw new IllegalStateException("This should never happen", e);
        }
    }

    private static URI buildDescribeURI(Authentication authentication, String entityName) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/data/v" + authentication.getApiVersion() + "/sobjects/" + entityName + "/describe",
                null,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen", e);
        }
    }

    private static URI buildSetPasswordURI(Authentication authentication, String userId) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/data/v" + authentication.getApiVersion() + "/sobjects/User/" + userId + "/password",
                null,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen", e);
        }
    }

    private static URI buildGetUserInfoURI(Authentication authentication) {
        URI instanceURI = URI.create(authentication.getInstanceUrl());
        try {
            return new URI(
                instanceURI.getScheme(),
                instanceURI.getAuthority(),
                "/services/oauth2/userinfo",
                null,
                null);

        } catch (URISyntaxException e) {
            throw new IllegalStateException("This should never happen", e);
        }
    }

    private static String buildCompositeSObjectsBody(List<SObject> sObjects) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        body.put("allOrNone", false);
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
                        throw new IllegalArgumentException("Unsupported SObject value class: " + valueClass.getSimpleName());
                    }
                }
            }
        });
        return jsonObject;
    }

    private static QueryResult buildQueryResult(JsonNode resultNode) {
        QueryResult result = new QueryResult();
        result.setDone(resultNode.get("done").asBoolean());
        result.setSize(resultNode.get("totalSize").asInt());
        if (resultNode.has("nextRecordsUrl")) {
            String nextRecordsUrl = resultNode.get("nextRecordsUrl").asText();
            String queryLocator = nextRecordsUrl.substring(nextRecordsUrl.lastIndexOf("/") + 1);
            result.setQueryLocator(queryLocator);
        }
        result.setRecords(buildSObjects(resultNode.get("records")).toArray(new SObject[0]));
        return result;
    }

    private static List<SObject> buildSObjects(JsonNode jsonObjects) {
        List<SObject> sObjects = new ArrayList<>();
        for (JsonNode sObject : jsonObjects) {
            sObjects.add(buildSObject(sObject));
        }
        return sObjects;
    }

    private static SObject buildSObject(JsonNode jsonObject) {
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
                        if (fieldNode.has("records")) {
                            sObject.setSObjectField(fieldName, buildQueryResult(fieldNode));

                        } else if (fieldNode.has("attributes")) {
                            sObject.setSObjectField(fieldName, buildSObject(fieldNode));

                        } else {
                            throw new IllegalArgumentException("Unsupported data type");
                        }
                        break;
                }
            }
        });
        return sObject;
    }

    private static List<UpsertResult> buildUpsertResults(JsonNode resultsNode) {
        List<UpsertResult> results = new ArrayList<>();
        for (JsonNode resultNode : resultsNode) {
            results.add(buildUpsertResult(resultNode));
        }
        return results;
    }

    private static UpsertResult buildUpsertResult(JsonNode resultNode) {
        UpsertResult result = new UpsertResult();
        result.setSuccess(resultNode.has("success") && resultNode.get("success").asBoolean());
        result.setCreated(result.isSuccess());
        if (resultNode.has("id")) {
            result.setId(resultNode.get("id").asText());
        }
        if (resultNode.has("errors")) {
            result.setErrors(buildErrors(resultNode.get("errors")).toArray(new IError[0]));
        }
        return result;
    }

    private static List<IError> buildErrors(JsonNode errorsNode) {
        List<IError> errors = new ArrayList<>();
        for (JsonNode errorNode : errorsNode) {
            errors.add(buildError(errorNode));
        }
        return errors;
    }

    private static IError buildError(JsonNode errorNode) {
        IError error = new com.sforce.soap.partner.Error();
        error.setStatusCode(StatusCode.valueOf(errorNode.get("statusCode").asText()));
        error.setMessage(errorNode.get("message").asText());
        if (errorNode.has("fields")) {
            error.setFields(stream(errorNode.get("fields").spliterator(), false).map(JsonNode::asText).toArray(String[]::new));
        }
        return error;
    }

    private static String buildSetPasswordBody(String password) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        body.put("NewPassword", password);
        return body.toString();
    }

    private static DescribeSObjectResult buildDescribeSObjectResult(JsonNode resultNode) {
        DescribeSObjectResult result = new DescribeSObjectResult();
        result.setActivateable(resultNode.get("activateable").booleanValue());
        result.setCompactLayoutable(resultNode.get("compactLayoutable").booleanValue());
        result.setCreateable(resultNode.get("createable").booleanValue());
        result.setCustom(resultNode.get("custom").booleanValue());
        result.setCustomSetting(resultNode.get("customSetting").booleanValue());
        result.setDeletable(resultNode.get("deletable").booleanValue());
        result.setDeprecatedAndHidden(resultNode.get("deprecatedAndHidden").booleanValue());
        result.setFeedEnabled(resultNode.get("feedEnabled").booleanValue());
        result.setKeyPrefix(resultNode.get("keyPrefix").textValue());
        result.setLabel(resultNode.get("label").textValue());
        result.setLabelPlural(resultNode.get("labelPlural").textValue());
        result.setLayoutable(resultNode.get("layoutable").booleanValue());
        result.setMergeable(resultNode.get("mergeable").booleanValue());
        result.setMruEnabled(resultNode.get("mruEnabled").booleanValue());
        result.setName(resultNode.get("name").textValue());
        result.setNetworkScopeFieldName(resultNode.get("networkScopeFieldName").textValue());
        result.setQueryable(resultNode.get("queryable").booleanValue());
        result.setReplicateable(resultNode.get("replicateable").booleanValue());
        result.setRetrieveable(resultNode.get("retrieveable").booleanValue());
        result.setSearchable(resultNode.get("searchable").booleanValue());
        result.setSearchLayoutable(resultNode.get("searchLayoutable").booleanValue());
        result.setTriggerable(resultNode.get("triggerable").booleanValue());
        result.setUndeletable(resultNode.get("undeletable").booleanValue());
        result.setUpdateable(resultNode.get("updateable").booleanValue());

        result.setFields(buildFields(resultNode.get("fields")).toArray(new IField[0]));

        //TODO And much, much more...

        return result;
    }

    private static List<IField> buildFields(JsonNode fieldsNode) {
        List<IField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldsNode) {
            fields.add(buildField(fieldNode));
        }
        return fields;
    }

    private static IField buildField(JsonNode fieldNode) {
        Field field = new Field();
        field.setAutoNumber(fieldNode.get("autoNumber").booleanValue());
        field.setByteLength(fieldNode.get("byteLength").intValue());
        field.setCalculated(fieldNode.get("calculated").booleanValue());
        field.setCaseSensitive(fieldNode.get("caseSensitive").booleanValue());
        field.setControllerName(fieldNode.get("controllerName").textValue());
        field.setCreateable(fieldNode.get("createable").booleanValue());
        field.setCustom(fieldNode.get("custom").booleanValue());
        field.setDefaultedOnCreate(fieldNode.get("defaultedOnCreate").booleanValue());
        field.setDefaultValueFormula(fieldNode.get("defaultValueFormula").textValue());
        field.setDependentPicklist(fieldNode.get("dependentPicklist").booleanValue());
        field.setDeprecatedAndHidden(fieldNode.get("deprecatedAndHidden").booleanValue());
        field.setDigits(fieldNode.get("digits").intValue());
        field.setDisplayLocationInDecimal(fieldNode.get("displayLocationInDecimal").booleanValue());
        field.setEncrypted(fieldNode.get("encrypted").booleanValue());
        field.setExtraTypeInfo(fieldNode.get("extraTypeInfo").textValue());
        field.setFilterable(fieldNode.get("filterable").booleanValue());
        field.setGroupable(fieldNode.get("groupable").booleanValue());
        field.setIdLookup(fieldNode.get("idLookup").booleanValue());
        field.setInlineHelpText(fieldNode.get("inlineHelpText").textValue());
        field.setLabel(fieldNode.get("label").textValue());
        field.setLength(fieldNode.get("length").intValue());
        field.setName(fieldNode.get("name").textValue());
        field.setNameField(fieldNode.get("nameField").booleanValue());
        field.setNamePointing(fieldNode.get("namePointing").booleanValue());
        field.setNillable(fieldNode.get("nillable").booleanValue());
        field.setPermissionable(fieldNode.get("permissionable").booleanValue());
        field.setPicklistValues(buildPicklistEntries(fieldNode.get("picklistValues")).toArray(new PicklistEntry[0]));
        field.setPolymorphicForeignKey(fieldNode.get("polymorphicForeignKey").booleanValue());
        field.setPrecision(fieldNode.get("precision").intValue());
        field.setRelationshipName(fieldNode.get("relationshipName").textValue());
        field.setRelationshipOrder(fieldNode.get("relationshipOrder").intValue());
        field.setReferenceTargetField(fieldNode.get("referenceTargetField").textValue());
        field.setRestrictedPicklist(fieldNode.get("restrictedPicklist").booleanValue());
        field.setScale(fieldNode.get("scale").intValue());
        field.setSearchPrefilterable(fieldNode.get("searchPrefilterable").booleanValue());
        field.setSoapType(SoapType.valueOf(SoapType.valuesToEnums.get(fieldNode.get("soapType").textValue())));
        field.setSortable(fieldNode.get("sortable").booleanValue());
        field.setType(FieldType.valueOf(FieldType.valuesToEnums.get(fieldNode.get("type").textValue())));
        field.setUnique(fieldNode.get("unique").booleanValue());
        field.setUpdateable(fieldNode.get("updateable").booleanValue());
        field.setWriteRequiresMasterRead(fieldNode.get("writeRequiresMasterRead").booleanValue());

        //TODO And more...

        return field;
    }

    private static List<PicklistEntry> buildPicklistEntries(JsonNode picklistEntriesNode) {
        List<PicklistEntry> picklistEntries = new ArrayList<>();
        for (JsonNode picklistEntryNode : picklistEntriesNode) {
            picklistEntries.add(buildPicklistEntry(picklistEntryNode));
        }
        return picklistEntries;
    }

    private static PicklistEntry buildPicklistEntry(JsonNode picklistEntryNode) {
        PicklistEntry picklistEntry = new PicklistEntry();
        picklistEntry.setActive(picklistEntryNode.get("active").booleanValue());
        picklistEntry.setDefaultValue(picklistEntryNode.get("defaultValue").booleanValue());
        picklistEntry.setLabel(picklistEntryNode.get("label").textValue());
        picklistEntry.setValue(picklistEntryNode.get("value").textValue());

        return picklistEntry;
    }

    private static GetUserInfoResult buildGetUserInfoResult(JsonNode resultNode) {
        GetUserInfoResult result = new GetUserInfoResult();
        result.setUserId(resultNode.get("user_id").textValue());
        result.setOrganizationId(resultNode.get("organization_id").textValue());
        result.setUserFullName(resultNode.get("name").textValue());
        result.setUserEmail(resultNode.get("email").textValue());
        result.setUserId(resultNode.get("user_id").textValue());
        result.setUserName(resultNode.get("preferred_username").textValue());
        result.setUserTimeZone(resultNode.get("zoneinfo").textValue());
        result.setUserLocale(resultNode.get("locale").textValue());
        result.setUserLanguage(resultNode.get("language").textValue());
        result.setUserType(resultNode.get("user_type").textValue());

        return result;
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
