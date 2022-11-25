package net.davidbuccola.commons.jersey;

import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.Response;

/**
 * A base runtime application exception indicating "Not Implemented" (HTTP {@code 501}).
 */
public final class NotImplementedException extends ServerErrorException {

    public NotImplementedException() {
        super(Response.Status.NOT_IMPLEMENTED);
    }

    public NotImplementedException(String message) {
        super(message, Response.Status.NOT_IMPLEMENTED);
    }

}
