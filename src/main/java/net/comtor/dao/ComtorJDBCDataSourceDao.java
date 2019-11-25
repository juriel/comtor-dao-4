package net.comtor.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
     * @param ds

     * @throws net.comtor.dao.ComtorDaoException
     * @throws java.sql.SQLException
     */
    public ComtorJDBCDataSourceDao(DataSource ds) throws ComtorDaoException, SQLException {
        super(ds.getConnection());
        this.dataSource = ds;
    }
//    
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
        setJdbcConnection(getDataSource().getConnection());
    }

    /**
     *
     * @return
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     *
     * @param rs
     * @param s
     * @param conn
     * @param dao
     */
    public static void safeClose(ResultSet rs, Statement stmt, Connection conn, ComtorJDBCDao dao) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }

        safeClose(stmt, conn, dao);
    }

    public static void safeClose(Statement stmt, Connection conn, ComtorJDBCDao dao) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
            }
        }

        safeClose(conn, dao);
    }

    public static void safeClose(Statement[] statements, Connection conn, ComtorJDBCDao dao) {
        if (statements != null) {
            for (Statement stmt : statements) {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception e) {
                    }
                }
            }
        }

        safeClose(conn, dao);
    }

    public static void safeClose(Connection conn, ComtorJDBCDao dao) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
            }
        }

        if (dao != null) {
            try {
                dao.close();
            } catch (Exception e) {
            }
        }
    }
}
