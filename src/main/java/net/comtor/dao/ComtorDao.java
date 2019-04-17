package net.comtor.dao;

import java.util.LinkedList;

/**
 * ComtorDao represents a repository of objects, such as database, XML file, binary file etc.
 *
 * it's available to stores and recover Element
 *
 */
public interface ComtorDao {

    /**
     * Updates element using element info and ComtoDaoKey from desc
     *
     * @param element
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    void updateElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     * Updates element using element info and ComtoDaoKey from key parameter
     *
     * @param element
     * @param key
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    void updateElement(Object element, ComtorDaoKey key, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @param key
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    Object findElement(ComtorDaoKey key, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @param element
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    void insertElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @param element
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    void deleteElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @param key
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    void deleteElement(ComtorDaoKey key, ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @throws net.comtor.dao.ComtorDaoException
     */
    void close() throws ComtorDaoException;

    /**
     *
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    long getNextId(ComtorDaoDescriptor desc) throws ComtorDaoException;

    /**
     *
     * @param query
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    LinkedList<Object> findAll(String query, ComtorDaoDescriptor desc, Object... params) throws ComtorDaoException;

    /**
     *
     * @param query
     * @param desc
     * @param firstElement
     * @param maxElements
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    LinkedList<Object> findAllRange(String query, ComtorDaoDescriptor desc, long firstElement, long maxElements, Object... params)
            throws ComtorDaoException;
    /**
     *
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    //String getFindQuery(ComtorDaoDescriptor desc) throws ComtorDaoException;
}
