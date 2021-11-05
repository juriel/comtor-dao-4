package net.comtor.dao;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * ComtorJDBCDataSourceDao uses apache DBCP	(Database connection pooling services) to improve database pooling
 *
 * @author COMTOR
 */
public class ComtorJDBCDataSourceDao extends ComtorJDBCDao {

    private DataSource dataSource;

    /**
     *
     * @param driver Class name of java.sql.Driver
     * @param url URL to database connection
     * @throws net.comtor.dao.ComtorDaoException
     */
    public ComtorJDBCDataSourceDao(String driver, String url) throws ComtorDaoException {
        super(driver, url);
    }

    /**
     *
     * @param driver Class name of java.sql.Driver
     * @param url URL to database connection
     * @param username
     * @param password
     * @throws net.comtor.dao.ComtorDaoException
     */
    public ComtorJDBCDataSourceDao(String driver, String url, String username, String password) throws ComtorDaoException {
        super(driver, url, username, password);
    }

    /**
     *
     * @param dataSource
     *
     * @throws net.comtor.dao.ComtorDaoException
     * @throws java.sql.SQLException
     */
    public ComtorJDBCDataSourceDao(DataSource dataSource) throws ComtorDaoException, SQLException {
        super(dataSource.getConnection());
        this.dataSource = dataSource;
    }

    /**
     *
     * @param driver Class name of java.sql.Driver
     * @param url URL to database connection
     * @param user username
     * @param password password
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    @Override
    protected void initConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        dataSource = DataSourceConnectionMap.getInstance().getDataSource(driver, url, user, password);
        DataSource ds = null;
        Connection conn = null;
        try {
             ds = getDataSource();
            conn = ds.getConnection();
            setJdbcConnection(conn);
        } catch (Exception e) {
            System.out.println("ERROR DRIVER " +driver);
            System.out.println("ERROR Datasource " +ds+" "+(ds != null?ds.getClass(): "NONE"));
            System.out.println("ERROR connection " +conn+" "+(conn != null?conn.getClass(): "NONE"));
            throw e;
        }

    }

    /**
     *
     * @return
     */
    public DataSource getDataSource() {
        return dataSource;
    }

}
