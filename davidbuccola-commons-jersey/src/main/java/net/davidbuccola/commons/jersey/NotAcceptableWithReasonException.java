package net.davidbuccola.commons.jersey;

import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.NOT_ACCEPTABLE;

/**
 * Enhanced version of {@link NotAcceptableException} that passes on a reason phrase. I have no idea why the standard
 * Jersey exception doesn't do this.
 */
public class NotAcceptableWithReasonException extends NotAcceptableException {

    public NotAcceptableWithReasonException(String reasonPhrase) {
        super(Response.status(NOT_ACCEPTABLE.getStatusCode(), NOT_ACCEPTABLE.getReasonPhrase() + ": " + reasonPhrase).build());
    }
}
