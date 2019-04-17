package net.comtor.dao;

public interface ComtorDaoElementAutoDescriptor {

    public ComtorDao getDao() throws ComtorDaoException;

    public ComtorDaoDescriptor getDaoDescriptor();

    /**
     * Inserts element in Dao
     *
     * @throws ComtorDaoException
     */
    public void insertInDao() throws ComtorDaoException;

    /**
     * Delete element in Dao
     *
     * @return
     * @throws ComtorDaoException
     */
    public void deleteInDao() throws ComtorDaoException;

    /**
     * Update Element inDao
     *
     * @throws ComtorDaoException
     */
    public void updateInDao() throws ComtorDaoException;

    /**
     * Find a Element inDao
     *
     * @throws ComtorDaoException
     */
    public Object findInDao(ComtorDaoKey key) throws ComtorDaoException;
}
