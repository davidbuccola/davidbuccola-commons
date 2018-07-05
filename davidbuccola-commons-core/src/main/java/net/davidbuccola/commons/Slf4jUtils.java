package net.davidbuccola.commons;

import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

/**
 * Utilities to help with SLF4J logging. This includes help with MDC as well as help with lazily built messages.
 */
public final class Slf4jUtils {

    private Slf4jUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static void error(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isErrorEnabled()) {
            logger.error(messageSupplier.get());
        }
    }

    public static void warn(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isWarnEnabled()) {
            logger.warn(messageSupplier.get());
        }
    }

    public static void info(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isInfoEnabled()) {
            logger.info(messageSupplier.get());
        }
    }

    public static void debug(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isTraceEnabled()) {
            logger.debug(messageSupplier.get());
        }
    }

    public static void trace(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isDebugEnabled()) {
            logger.trace(messageSupplier.get());
        }
    }

    public static void error(Logger logger, String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isErrorEnabled()) {
            logger.error(buildMessageWithData(baseMessage, dataSupplier));
        }
    }

    public static void warn(Logger logger, String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isWarnEnabled()) {
            logger.warn(buildMessageWithData(baseMessage, dataSupplier));
        }
    }

    public static void info(Logger logger, String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isInfoEnabled()) {
            logger.info(buildMessageWithData(baseMessage, dataSupplier));
        }
    }

    public static void debug(Logger logger, String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isDebugEnabled()) {
            logger.debug(buildMessageWithData(baseMessage, dataSupplier));
        }
    }

    public static void trace(Logger logger, String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isTraceEnabled()) {
            logger.trace(buildMessageWithData(baseMessage, dataSupplier));
        }
    }

    public static void error(Logger logger, String baseMessage, Throwable e, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isErrorEnabled()) {
            logger.error(buildMessageWithData(baseMessage, dataSupplier), e);
        }
    }

    public static void warn(Logger logger, String baseMessage, Throwable e, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isWarnEnabled()) {
            logger.warn(buildMessageWithData(baseMessage, dataSupplier), e);
        }
    }

    public static void info(Logger logger, String baseMessage, Throwable e, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isInfoEnabled()) {
            logger.info(buildMessageWithData(baseMessage, dataSupplier), e);
        }
    }

    public static void debug(Logger logger, String baseMessage, Throwable e, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isDebugEnabled()) {
            logger.debug(buildMessageWithData(baseMessage, dataSupplier), e);
        }
    }

    public static void trace(Logger logger, String baseMessage, Throwable e, Supplier<Map<String, Object>> dataSupplier) {
        if (logger.isTraceEnabled()) {
            logger.trace(buildMessageWithData(baseMessage, dataSupplier), e);
        }
    }

    public static void doWithMDCContext(String key, String value, Runnable logic) {
        Map<String, String> previousMDCContext = copyOfCurrentMDCContext();
        try {
            MDC.put(key, value);
            logic.run();
        } finally {
            MDC.setContextMap(previousMDCContext);
        }
    }

    public static <T> T doWithMDCContext(String key, String value, Callable<T> logic) {
        Map<String, String> previousMDCContext = copyOfCurrentMDCContext();
        try {
            MDC.put(key, value);
            return logic.call();

        } catch (RuntimeException e) {
            throw e;

        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            MDC.setContextMap(previousMDCContext);
        }
    }

    private static Map<String, String> copyOfCurrentMDCContext() {
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        return mdcContext != null ? mdcContext : emptyMap();
    }

    private static String buildMessageWithData(String baseMessage, Supplier<Map<String, Object>> dataSupplier) {
        try {
            String dataPart = dataSupplier.get().entrySet().stream()
                .map(entry -> entry.getKey() + "=" + String.valueOf(entry.getValue()))
                .collect(joining(", "));

            return baseMessage + ", " + dataPart;

        } catch (Exception e) {
            return "Failed to build message '" + baseMessage + "' because of " + e.toString();
        }
    }
}
