package net.comtor.dao;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

/**
 * singleton of All DataSources
 *
 * @author COMTOR
 */
public class DataSourceConnectionMap {

    //public static int MAX_NUM_OF_POOL_CONNECTIONS = 20;
    public static int  MAX_IDLE =20;
    public static int MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS = 60000;
    public static int VALIDATION_QUERY_TIMEOUT = 1;  //seconds
    public static int MAX_ACTIVE = -1;
    public static int MIN_IDLE = 0;

    private HashMap<String, javax.sql.DataSource> dataSourceMap;
    static DataSourceConnectionMap instance;

    static {
        instance = new DataSourceConnectionMap();
    }

    private DataSourceConnectionMap() {
        dataSourceMap = new HashMap<>();
    }

    public synchronized DataSource getDataSource(String driver, String url, String user, String password) {
        String key = "[" + driver + "][" + url + "][" + user + "][" + password + "]";
        DataSource dataSource = dataSourceMap.get(key);

        if (dataSource == null) {

            BasicDataSource basicDataSource = new BasicDataSource();


            //basicDataSource.setMaxActive(MAX_NUM_OF_POOL_CONNECTIONS);

            basicDataSource.setDriverClassName(driver);
            basicDataSource.setUsername(user);
            basicDataSource.setPassword(password);
            basicDataSource.setUrl(url);
            basicDataSource.setMaxIdle(MAX_IDLE);
            basicDataSource.setMaxActive(MAX_ACTIVE);
            basicDataSource.setMinIdle(MIN_IDLE);
            
           // basicDataSource.setMaxActive(MAX_NUM_OF_POOL_CONNECTIONS);

            basicDataSource.setValidationQueryTimeout(VALIDATION_QUERY_TIMEOUT);
            basicDataSource.setTestOnBorrow(true);
            //basicDataSource.setMaxWait(20000);
            basicDataSource.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS);
            basicDataSource.setValidationQuery(getValidationQuery(driver));
            basicDataSource.setTestWhileIdle(true);

            dataSourceMap.put(key, basicDataSource);

            return basicDataSource;
        }

        return dataSource;
    }

    static DataSourceConnectionMap getInstance() {
        return instance;
    }

    public static void destroyInstance() {
        if (instance != null) {
            try {
                instance.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        instance = null;
    }

    private void close() throws SQLException {
        Set<String> keys = dataSourceMap.keySet();
        for (String k : keys) {
            DataSource ds = dataSourceMap.get(k);
            if (ds != null && ds instanceof BasicDataSource) {
                BasicDataSource bds = (BasicDataSource) ds;
                bds.close();
            }
        }
    }

    private static String getValidationQuery(final String driver) {
        switch (driver) {
            case ComtorJDBCDao.DRIVER_POSTGRES:
                return "SELECT 1+1";
            case ComtorJDBCDao.DRIVER_MYSQL:
            case ComtorJDBCDao.DRIVER_MARIADB:
                return "SELECT 1+1";
            case ComtorJDBCDao.DRIVER_ORACLE:
            case ComtorJDBCDao.DRIVER_ORACLE_2:
                return "SELECT 1+1 FROM DUAL";
            case ComtorJDBCDao.DRIVER_SQL_SERVER:
                return "SELECT 1+1";
            default:
                return "";
        }
    }
}
