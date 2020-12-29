package net.comtor.dao;

import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

/**
 * singleton of All DataSources
 *
 * @author COMTOR
 */
public class DataSourceConnectionMap {

    public static int MAX_NUM_OF_POOL_CONNECTIONS = 8;
    public static int MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS = 60000;
    private HashMap<String, javax.sql.DataSource> dataSourceMap;
    static DataSourceConnectionMap instance;

    static {
        instance = new DataSourceConnectionMap();
    }

    private DataSourceConnectionMap() {
        dataSourceMap = new HashMap<String, DataSource>();
    }

    public synchronized DataSource getDataSource(String driver, String url, String user, String password) {
        String key = "[" + driver + "][" + url + "][" + user + "][" + password + "]";
        DataSource dataSource = dataSourceMap.get(key);

        if (dataSource == null) {
            BasicDataSource basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName(driver);
            basicDataSource.setUsername(user);
            basicDataSource.setPassword(password);
            basicDataSource.setUrl(url);
            basicDataSource.setMaxIdle(MAX_NUM_OF_POOL_CONNECTIONS);
            basicDataSource.setMinIdle(0);
            basicDataSource.setMaxActive(MAX_NUM_OF_POOL_CONNECTIONS);
            basicDataSource.setValidationQueryTimeout(30);
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
        instance = null;
    }

    private static String getValidationQuery(final String driver) {
        switch (driver) {
            case ComtorJDBCDao.DRIVER_POSTGRES:
                return "SELECT 1+1";
            case ComtorJDBCDao.DRIVER_MYSQL:
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
