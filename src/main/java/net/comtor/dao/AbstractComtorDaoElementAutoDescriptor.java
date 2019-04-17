package net.comtor.dao;

/**
 *
 * @author comtor@comtor.net
 */
public abstract class AbstractComtorDaoElementAutoDescriptor implements ComtorDaoElementAutoDescriptor {

    @Override
    public void insertInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            preInsert(dao);
            dao.insertElement(this, getDaoDescriptor());
            postInsert(dao);
        } finally {
            safeDaoClose(dao);
        }
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

    @Override
    public void deleteInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            dao.deleteElement(this, getDaoDescriptor());
        } finally {
            safeDaoClose(dao);
        }
    }

    @Override
    public void updateInDao() throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            dao.updateElement(this, getDaoDescriptor());
        } finally {
            safeDaoClose(dao);
        }
    }

    @Override
    public Object findInDao(ComtorDaoKey key) throws ComtorDaoException {
        ComtorDao dao = null;
        try {
            dao = getDao();
            Object obj = dao.findElement(key, getDaoDescriptor());
            return obj;
        } finally {
            safeDaoClose(dao);
        }
    }

    /**
     *
     * @param dao
     */
    private void safeDaoClose(ComtorDao dao) {
        if (dao != null) {
            try {
                dao.close();
            } catch (ComtorDaoException e) {
            }
        }
    }
}
