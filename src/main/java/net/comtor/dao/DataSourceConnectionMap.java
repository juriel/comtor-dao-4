package net.comtor.dao;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * singleton of All DataSources
 *
 * @author COMTOR
 */
public class DataSourceConnectionMap {

    //public static int MAX_NUM_OF_POOL_CONNECTIONS = 20;
    public static int MAX_IDLE = 20;
    public static int MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS = 60000;
    public static int VALIDATION_QUERY_TIMEOUT = 1;  //seconds
    public static int MAX_TOTAL = -1;
    public static int MIN_IDLE = 0;
    public static int MAX_WAIT_MILLIS = -1;
    public static int TIME_BETWEEN_EVICTION_RUNS_MILLIS = -1;
    public static boolean TEST_ON_BORROW = true;
    public static boolean TEST_WHILE_IDLE = true;
    public static boolean LOG_ABANDONED = true;

    private final HashMap<String, javax.sql.DataSource> dataSourceMap;
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
            basicDataSource.setMaxTotal(MAX_TOTAL);
            basicDataSource.setMinIdle(MIN_IDLE);

            // basicDataSource.setMaxActive(MAX_NUM_OF_POOL_CONNECTIONS);
            basicDataSource.setValidationQueryTimeout(VALIDATION_QUERY_TIMEOUT);
            basicDataSource.setTestOnBorrow(TEST_ON_BORROW);
            //basicDataSource.setMaxWait(20000);
            basicDataSource.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIMEOUT_MILLIS);
            basicDataSource.setValidationQuery(getValidationQuery(driver));
            basicDataSource.setTestWhileIdle(TEST_WHILE_IDLE);
//            System.err.println("============================Setting logging================================");
//            System.out.println("============================Setting logging out================================");
            basicDataSource.setLogAbandoned(LOG_ABANDONED);
            basicDataSource.setMaxWaitMillis(MAX_WAIT_MILLIS);
            if (TIME_BETWEEN_EVICTION_RUNS_MILLIS > -1) {
                basicDataSource.setTimeBetweenEvictionRunsMillis(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
            }
            try {
                basicDataSource.setLogWriter(new PrintWriter("/tmp/pool.txt"));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DataSourceConnectionMap.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SQLException ex) {
                Logger.getLogger(DataSourceConnectionMap.class.getName()).log(Level.SEVERE, null, ex);
            }

            dataSourceMap.put(key, basicDataSource);

            return basicDataSource;
        }
//        BasicDataSource ds = (BasicDataSource) dataSource;
//        System.out.println("==========> active:" + ds.getNumActive() + "  idle:" + ds.getNumIdle() + " <==================");
////        ds.setLogExpiredConnections(true);
////        ds.setLogAbandoned(true);
////        
////        Thread th = Thread.currentThread();
////        StackTraceElement[] st = th.getStackTrace();
////        for (StackTraceElement s : st) {
////            if (s.getClassName().startsWith("org.apache.jsp")){
////                break;
////            }
////            System.out.println("     " + s.getClassName() + ":" + s.getMethodName() + " " + s.getLineNumber());
////        }
////        System.out.println("-------------------------------------------------------------------------------------");

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
