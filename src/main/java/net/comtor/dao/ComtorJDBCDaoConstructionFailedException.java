package net.comtor.dao;

/**
 *
 * @author jorgegarcia@comtor.net
 * @since Mar 13, 2015
 */
public class ComtorJDBCDaoConstructionFailedException
        extends ComtorDaoException {

    public ComtorJDBCDaoConstructionFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
