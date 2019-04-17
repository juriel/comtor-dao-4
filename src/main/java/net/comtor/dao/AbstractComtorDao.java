package net.comtor.dao;

/**
 *
 * @author comtor@comtor.net
 * @since July 4, 2007
 */
public abstract class AbstractComtorDao implements ComtorDao {

    @Override
    public void updateElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException {
        updateElement(element, desc.getKey(element), desc);
    }

    @Override
    public void deleteElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException {
        deleteElement(desc.getKey(element), desc);
    }
}
