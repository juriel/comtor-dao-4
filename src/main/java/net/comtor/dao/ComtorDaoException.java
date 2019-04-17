package net.comtor.dao;

import net.comtor.exception.ComtorException;

/**
 *
 * Exception that encapsulates any exception
 *
 * @author juriel
 */
public class ComtorDaoException extends ComtorException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of BusinessLogicException
     */
    public ComtorDaoException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of BusinessLogicException
     */
    public ComtorDaoException(Throwable throwable) {
        super(throwable);
    }

    public ComtorDaoException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
