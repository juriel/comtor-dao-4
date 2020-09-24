/*
 * ComtorException.java
 *
 * Created on July 19, 2007, 4:21 AM
 */
package net.comtor.exception;

import java.io.IOException;

public class ComtorException extends IOException {

    private static final long serialVersionUID = 1L;

    private String message = null;
    private Throwable originalException = null;

    /**
     * Creates a new instance of BusinessLogicException
     */
    public ComtorException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of BusinessLogicException
     */
    public ComtorException(Throwable throwable) {
        super(throwable);

        this.originalException = throwable;
    }

    public ComtorException(String message, Throwable throwable) {
        super(throwable);

        this.message = message;
        this.originalException = throwable;
    }

    public String getMessage() {
        if (message == null) {
            return super.getMessage();
        }

        return message;
    }

    public Throwable getOriginalException() {
        return originalException;
    }

    public String getOriginalMessage() {
        if (originalException == null) {
            return "No original message";
        }

        return originalException.getMessage();
    }
}
