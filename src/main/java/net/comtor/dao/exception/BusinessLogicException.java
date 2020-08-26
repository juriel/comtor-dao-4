package net.comtor.dao.exception;

import net.comtor.exception.ComtorException;

/**
 *
 * @author jorgegarcia
 */
public class BusinessLogicException extends ComtorException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of BusinessLogicException
     *
     * @param message
     */
    public BusinessLogicException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of BusinessLogicException
     *
     * @param throwable
     */
    public BusinessLogicException(Throwable throwable) {
        super(throwable);
    }

    /**
     *
     * @param message
     * @param throwable
     */
    public BusinessLogicException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
