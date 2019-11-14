package net.comtor.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * ComtorJDBCDataSourceDao uses apache DBCP	(Database connection pooling
 * services) to improve database pooling
 *
 * @author COMTOR
 */
public class ComtorJDBCDataSourceDao extends ComtorJDBCDao {

    private static final Logger LOG = Logger.getLogger(ComtorJDBCDataSourceDao.class.getName());

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
    public ComtorJDBCDataSourceDao(String driver, String url, String username,
            String password) throws ComtorDaoException {
        super(driver, url, username, password);
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
    protected void initConnection(String driver, String url, String user, String password)
            throws ClassNotFoundException, SQLException {
        dataSource = DataSourceConnectionMap.getInstance()
                .getDataSource(driver, url, user, password);

        setJdbcConnection(getDataSource().getConnection());
    }

    /**
     *
     * @return
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    public static void safeClose(ComtorJDBCDao dao, Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }

        if (dao != null) {
            try {
                dao.close();
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

}
