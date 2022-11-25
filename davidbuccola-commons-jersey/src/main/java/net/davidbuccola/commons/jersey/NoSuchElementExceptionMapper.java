package net.davidbuccola.commons.jersey;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.NoSuchElementException;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Provider
public final class NoSuchElementExceptionMapper implements ExceptionMapper<NoSuchElementException> {

    @Override
    public Response toResponse(NoSuchElementException exception) {
        return Response.status(NOT_FOUND).build();
    }
}
