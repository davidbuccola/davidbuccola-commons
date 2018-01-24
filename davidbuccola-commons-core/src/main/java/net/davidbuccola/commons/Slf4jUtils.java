package net.davidbuccola.force.streaming.core.util;

import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

/**
 * Utilities to help with Slf4j logging. This includes help with MDC as well as help with lazily evaluated messages.
 */
public final class Slf4jUtils {

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

    public static void doWithMDCContext(String extraKey1, String extraValue1, Runnable logic) {
        Map<String, String> previousMDCContext = getCopyOfCurrentMDCContext();
        try {
            MDC.put(extraKey1, extraValue1);

            logic.run();
        } finally {
            MDC.setContextMap(previousMDCContext);
        }
    }

    public static void doWithMDCContext(String extraKey1, String extraValue1, String extraKey2, String extraValue2, Runnable logic) {
        Map<String, String> previousMDCContext = getCopyOfCurrentMDCContext();
        try {
            MDC.put(extraKey1, extraValue1);
            MDC.put(extraKey2, extraValue2);
            logic.run();
        } finally {
            MDC.setContextMap(previousMDCContext);
        }
    }

    private static Map<String, String> getCopyOfCurrentMDCContext() {
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
