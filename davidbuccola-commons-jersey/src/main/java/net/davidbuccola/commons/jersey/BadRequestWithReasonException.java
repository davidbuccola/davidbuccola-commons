package net.davidbuccola.commons.jersey;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

/**
 * Enhanced version of {@link BadRequestException} that passes on a reason phrase. I have no idea why the standard
 * Jersey exception doesn't do this.
 */
public class BadRequestWithReasonException extends BadRequestException {

    public BadRequestWithReasonException(String reasonPhrase) {
        super(Response.status(BAD_REQUEST.getStatusCode(), BAD_REQUEST.getReasonPhrase() + ": " + reasonPhrase).build());
    }
}
