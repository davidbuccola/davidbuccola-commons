package net.davidbuccola.commons.jersey;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;

@Provider
public final class UnsupportedOperationExceptionMapper implements ExceptionMapper<UnsupportedOperationException> {

    @Override
    public Response toResponse(UnsupportedOperationException exception) {
        return Response.status(NOT_IMPLEMENTED.getStatusCode(), exception.getMessage()).build();
    }
}
