/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.comtor.dao.generics;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import net.comtor.dao.ComtorDao;
import net.comtor.dao.ComtorDaoDescriptor;
import net.comtor.dao.ComtorDaoElementAutoDescriptor;
import net.comtor.dao.ComtorDaoException;
import net.comtor.dao.ComtorDaoKey;
import net.comtor.dao.annotations.AnnotationsJDBCDaoDescriptor;

/**
 *
 * @author dwin
 */
public abstract class ComtorDaoElement<T extends Serializable> implements ComtorDaoElementAutoDescriptor {

    private Class<T> entityBeanType;

    public ComtorDaoElement() {
        this.entityBeanType = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * @param dao
     * @throws ComtorDaoException
     */
    protected void postInsert(ComtorDao dao) throws ComtorDaoException {
    }

    /**
     * @param dao
     * @throws ComtorDaoException
     */
    protected void preInsert(ComtorDao dao) throws ComtorDaoException {
    }

    /**
     *
     * @return
     */
    public ComtorDaoDescriptor getDaoDescriptor() {
        try {
            return new AnnotationsJDBCDaoDescriptor(entityBeanType);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void insertInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            this.preInsert(dao);
            dao.insertElement(this, this.getDaoDescriptor());
            this.postInsert(dao);
        } catch (ComtorDaoException e) {
            throw e;
        } finally {
            safeDaoClose(dao);
        }
    }

    /**
     *
     * @return @throws net.comtor.dao.ComtorDaoException
     */
    public void deleteInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            dao.deleteElement(this, this.getDaoDescriptor());
        } catch (ComtorDaoException e) {
            throw e;
        } finally {
            safeDaoClose(dao);
        }
    }

    /**
     *
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void updateInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            dao.updateElement(this, this.getDaoDescriptor());
        } catch (ComtorDaoException e) {
            throw e;
        } finally {
            safeDaoClose(dao);
        }
    }

    /**
     *
     * @param key
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public T findInDao(ComtorDaoKey key) throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            Object obj = dao.findElement(key, this.getDaoDescriptor());
            if (entityBeanType.isInstance(obj)) {
                return (T) obj;
            }
        } catch (ComtorDaoException e) {
            throw e;
        } finally {
            safeDaoClose(dao);
        }
        return null;
    }

    /**
     * @param dao
     */
    private void safeDaoClose(ComtorDao dao) {
        try {
            if (dao != null) {
                dao.close();
            }
        } catch (ComtorDaoException e) {
            e.printStackTrace();
        }
    }
}
