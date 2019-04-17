package net.comtor.dao;

import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;

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
        DataSource ds = dataSourceMap.get(key);
        if (ds == null) {
            BasicDataSource basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName(driver);
            basicDataSource.setUsername(user);
            basicDataSource.setPassword(password);
            basicDataSource.setUrl(url);
            basicDataSource.setMaxIdle(MAX_NUM_OF_POOL_CONNECTIONS);
            basicDataSource.setMinIdle(0);
            basicDataSource.setMaxTotal(MAX_NUM_OF_POOL_CONNECTIONS);
            basicDataSource.setValidationQueryTimeout(30);
            //basicDataSource.setMaxWait(20000);
            basicDataSource.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS);
            if (driver.equals(ComtorJDBCDao.DRIVER_POSTGRES)) {
                basicDataSource.setValidationQuery("SELECT 1+1");
            } else if (driver.equals(ComtorJDBCDao.DRIVER_MYSQL)) {
                basicDataSource.setValidationQuery("SELECT 1+1");
            } else if (driver.equals(ComtorJDBCDao.DRIVER_ORACLE) || driver.equals(ComtorJDBCDao.DRIVER_ORACLE_2)) {
                basicDataSource.setValidationQuery("SELECT 1+1 FROM DUAL");
            } else if (driver.equals(ComtorJDBCDao.DRIVER_SQL_SERVER)) {
                basicDataSource.setValidationQuery("SELECT 1+1");
            }
            basicDataSource.setTestWhileIdle(true);

            dataSourceMap.put(key, basicDataSource);
            return basicDataSource;
        }
        return ds;
    }

    static DataSourceConnectionMap getInstance() {
        return instance;
    }

    public static void destroyInstance() {
        instance = null;
    }
}
