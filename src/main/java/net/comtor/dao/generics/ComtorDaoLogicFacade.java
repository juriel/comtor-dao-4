package net.comtor.dao.generics;

import java.io.Serializable;
import java.util.LinkedList;
import net.comtor.dao.ComtorDaoException;
import net.comtor.dao.ComtorJDBCDao;
import net.comtor.dao.ComtorJDBCDaoDescriptor;

/**
 *
 * @author dwin
 * @param <E>
 * @param <PK>
 */
public interface ComtorDaoLogicFacade<E, PK extends Serializable> {

    E find(PK id) throws ComtorDaoException;

    E findByProperty(String property, Object value) throws ComtorDaoException;

    /**
     * Finds all entities by one property.
     *
     * @param property Property name.
     * @param value Property's value.
     * @return List of entities that have the specified property's value.
     * @throws ComtorDaoException
     * @since May 27, 2015
     */
    LinkedList<E> findAllByProperty(String property, Object value)
            throws ComtorDaoException;

    void create(E entity) throws ComtorDaoException;

    void edit(E entity) throws ComtorDaoException;

    void remove(E entity) throws ComtorDaoException;

    public LinkedList<E> findAll(String queryString, Object... params) throws ComtorDaoException;

    public LinkedList<E> findAll(String queryString, long firsResult, long maxResults, Object... params) throws ComtorDaoException;

    String getFindQuery() throws ComtorDaoException;

    ComtorJDBCDaoDescriptor getTableDescriptorType() throws ComtorDaoException;

    long getCountElements(String queryString, Object... params) throws ComtorDaoException;

    ComtorJDBCDao getComtorJDBCDao() throws ComtorDaoException;

//    void loadChildElements(E entity, String nameField);
}
