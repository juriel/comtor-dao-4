package net.comtor.dao;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.comtor.dao.annotations.ComtorId;
import net.comtor.reflection.ReflectionHelper;

/**
 * Implementes ComtorDao using java.sql.Connection
 *
 * @author juriel and dwinpaez
 */
public class ComtorJDBCDao extends AbstractComtorDao {

    private static final Logger LOG = Logger.getLogger(ComtorJDBCDao.class.getName());

    public static boolean LOG_EXECUTE_QUERY = false;

    public static final String DRIVER_SQL_SERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DRIVER_POSTGRES  = "org.postgresql.Driver";
    public static final String DRIVER_MYSQL     = "com.mysql.jdbc.Driver";
    public static final String DRIVER_MARIADB   = "org.mariadb.jdbc.Driver";
    public static final String DRIVER_ORACLE    = "oracle.jdbc.OracleDriver";
    public static final String DRIVER_ORACLE_2  = "oracle.jdbc.driver.OracleDriver";
    public static final String DRIVER_SYBASE    = "com.sybase.jdbc2.jdbc.SybDriver";
    public static final String DRIVER_SQLITE    = "org.sqlite.JDBC";

    public static final String MYSQL_SEQUENCE = "SELECT LAST_INSERT_ID()";
    public static final String SQL_SERVER_SEQUENCE = "SELECT @@IDENTITY";
    public static final String SQLITE_SEQUENCE = "SELECT last_insert_rowid()";

    private Connection jdbcConnection = null;
    private String driver;
    private String url;
    private String user;
    private String password;

    public ComtorJDBCDao(Connection conn) throws ComtorDaoException {
        setJdbcConnection(conn);
    }

    /**
     *
     * @param driver
     * @param url
     * @throws net.comtor.dao.ComtorDaoException
     */
    public ComtorJDBCDao(String driver, String url) throws ComtorDaoException {
        super();

        this.driver = driver;
        this.url = url;

        try {
            initConnection(driver, url, null, null);
        } catch (Exception ex) {
            throw new ComtorJDBCDaoConstructionFailedException("ComtorJDBCDao construction failed " + ex.getMessage(), ex);
        }
    }

    /**
     *
     * @param driver
     * @param url
     * @param user
     * @param password
     * @throws net.comtor.dao.ComtorDaoException
     */
    public ComtorJDBCDao(String driver, String url, String user, String password) throws ComtorDaoException {
        super();

        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;

        try {
            initConnection(driver, url, user, password);
        } catch (Exception ex) {
            throw new ComtorJDBCDaoConstructionFailedException("ComtorJDBCDao construction failed (" + url + "," + " " + user + " , xxxxx , " + ex, ex);
        }
    }

    public String getDriver() {
        return driver;
    }

    /**
     *
     * @param jdbc
     */
    final protected void setJdbcConnection(Connection jdbc) {
        if (jdbcConnection != null) {
            try {
                System.err.println("JDBC Connection Already exists");
                jdbcConnection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        jdbcConnection = jdbc;
    }

    /**
     *
     * @param driver
     * @param url
     * @param user
     * @param password
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    protected void initConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = (user == null) ? DriverManager.getConnection(url) : DriverManager.getConnection(url, user, password);
        setJdbcConnection(conn);
        
    }

    /**
     * close jdbc Connnection
     */
    @Override
    public void close() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }

            jdbcConnection = null;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * finds element in database
     *
     * @param comtorDaoKey
     * @param descriptor
     * @return Object
     * @throws net.comtor.dao.ComtorDaoException
     */
    public Object findElement(ComtorDaoKey comtorDaoKey, ComtorDaoDescriptor descriptor) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor castedDescriptor = castDescriptor(descriptor);
        String keyValueStr = "";
        String sql = getFindQuery(comtorDaoKey, castedDescriptor);

        if (LOG_EXECUTE_QUERY) {
            System.out.println("SQL >> " + sql);
        }

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(sql)) {
            int i = 0;

            for (String key : comtorDaoKey.getKeys().keySet()) {
                int pos = i + 1;
                ComtorJDBCField field = castedDescriptor.getField(key);
                Class type = field.getType();
                Object value = comtorDaoKey.getValue(field.getAttributeName());
                keyValueStr += "" + field.getAttributeName() + " " + value;
                assignValueInPreparedStatement(ps, pos, type, value);

                i++;
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }

                Object result = descriptor.getObjectClass().newInstance();
                fillObject(result, rs, castedDescriptor);

                if (rs.next()) {
                    throw new ComtorDaoException("findElement founds more than one element. [" + sql + "]" + keyValueStr);
                }

                return result;
            }
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Can't create prepareStatement: " + sql + " " + ex.getMessage(), ex);
        } catch (InstantiationException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Can't Instantiate: " + ex.getMessage(), ex);
        } catch (IllegalAccessException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
        }

        return null;
    }

    /**
     * fillObject with Resultset dataFromResultSet.
     *
     * @param result
     * @param rs
     * @param daoDescriptor
     */
    public static void fillObject(Object result, ResultSet rs, ComtorJDBCDaoDescriptor daoDescriptor) {
        LinkedList<ComtorJDBCField> selectableFields = daoDescriptor.getSelectableFields();
        LinkedList<ComtorJDBCField> specialFields = daoDescriptor.getSpecialFields(); // ComtorSpecialField 2014-02-10
        selectableFields.addAll(specialFields);

        for (ComtorJDBCField selectableField : selectableFields) {
            Class fieldType = selectableField.getType();
            String columnNameFromField = selectableField.getColumnName();
            Object[] obj = new Object[1];  // parameter for invoke

            try {
                Object dataFromResultSet = rs.getObject(columnNameFromField);

                if (dataFromResultSet != null) {
                    obj[0] = dataFromResultSet;

                    try {
                        Class dataType = dataFromResultSet.getClass();

                        if ((fieldType.equals(long.class) || fieldType.equals(double.class)) && dataFromResultSet == null) {
                            selectableField.getSetMethod().invoke(result, 0);
                        } else if (fieldType.equals(long.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).longValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(long.class) && dataType.equals(BigInteger.class)) {
                            obj[0] = ((BigInteger) dataFromResultSet).longValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Long.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).longValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Long.class) && dataType.equals(BigInteger.class)) {
                            obj[0] = ((BigInteger) dataFromResultSet).longValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(double.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).doubleValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Double.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).doubleValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if ((fieldType.equals(float.class) || fieldType.equals(Float.class)) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).floatValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if ((fieldType.equals(int.class) || fieldType.equals(Integer.class)) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).intValue();
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(long.class) && dataType.equals(Boolean.class)) {
                            if (((Boolean) dataFromResultSet) == true) {
                                obj[0] = 1;
                            } else if (((Boolean) dataFromResultSet) == false) {
                                obj[0] = 0;
                            } else {
                                obj[0] = -1;
                            }

                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Timestamp.class)) {
                            obj[0] = rs.getTimestamp(columnNameFromField);
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Time.class)) {
                            obj[0] = rs.getTime(columnNameFromField);
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Date.class)) {
                            obj[0] = rs.getDate(columnNameFromField);
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(String.class) && dataFromResultSet instanceof java.sql.Clob) {
                            obj[0] = rs.getString(columnNameFromField);
                            selectableField.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
                            obj[0] = rs.getBoolean(columnNameFromField);
                            selectableField.getSetMethod().invoke(result, obj);
                        } else {
                            selectableField.getSetMethod().invoke(result, obj);
                        }
                    } catch (Exception ex) {
                        System.err.println("ERROR FILL  table " + daoDescriptor.getTableName() + " :  column " + selectableField.getColumnName() + " ("
                                + fieldType + ") : " + dataFromResultSet.getClass());

                        if (obj[0] != null) {
                            System.err.println("ResultSet value class " + obj[0].getClass());
                        } else {
                            System.err.println("ResultSet value is null");
                        }

                        System.err.println(ex.getClass().getName() + " : " + ex.getMessage());

                        LOG.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                }
            } catch (SQLException ex) {
                
                LOG.log(Level.SEVERE,"  columnNameFromField:"+ columnNameFromField+" " +ex.getMessage(), ex);
            } catch (IllegalArgumentException ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

    /**
     * @param ps Preparered Statement
     * @param pos prepared statement position
     * @param type field.getType()
     * @param value field.getValue(element)
     * @throws SQLException
     */
    public static void assignValueInPreparedStatement(PreparedStatement ps, int pos, Class type, Object value) throws SQLException {
        try {
            if (type.equals(String.class)) {
                ps.setString(pos, (String) value);
            } else if (type.equals(short.class)) {
                ps.setShort(pos, ((Short) value));
            } else if (type.equals(int.class) && value instanceof java.lang.Integer) {
                if (value == null) {
                    ps.setNull(pos, java.sql.Types.INTEGER);

                    return;
                }

                ps.setInt(pos, ((Integer) value));
            } else if (type.equals(int.class) && value instanceof java.lang.Long) {
                if (value == null) {
                    ps.setNull(pos, java.sql.Types.INTEGER);

                    return;
                }

                ps.setLong(pos, ((Long) value));
            } else if (type.equals(long.class) && value instanceof java.lang.Long) {
                if (value == null) {
                    ps.setNull(pos, java.sql.Types.INTEGER);

                    return;
                }

                ps.setLong(pos, ((Long) value));
            } else if (type.equals(long.class) && value instanceof java.lang.Integer) {
                if (value == null) {
                    ps.setNull(pos, java.sql.Types.INTEGER);

                    return;
                }

                ps.setLong(pos, ((Integer) value));
            } else if (type.equals(boolean.class)) {
                ps.setBoolean(pos, ((Boolean) value));
            } else if (type.equals(double.class)) {
                ps.setDouble(pos, ((Double) value));
            } else if (type.equals(float.class)) {
                ps.setDouble(pos, ((Float) value));
            } else if (type.equals(char.class)) {
                ps.setDouble(pos, ((Character) value));
            } else if (type.equals(Date.class)) {
                ps.setDate(pos, (Date) value);
            } else if (type.equals(Timestamp.class)) {
                ps.setTimestamp(pos, (Timestamp) value);
            } else if (type.equals(Time.class)) {
                ps.setTime(pos, (Time) value);
            } else if (type.equals(char.class)) {
                ps.setDouble(pos, ((Character) value));
            } else if (type.equals(byte[].class)) {
                ps.setBytes(pos, (byte[]) value);
            } else if (type.equals(BigDecimal.class)) {
                ps.setBigDecimal(pos, (BigDecimal) value);
            } else {
                ps.setObject(pos, value);
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            System.err.println("assignValueInPreparedStatement VALUES CLASS " + value.getClass() + " - " + pos);
            System.err.println("assignValueInPreparedStatement type " + type + " - " + value);
        }
    }

    /**
     *
     * @param element
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void insertElement(Object element, ComtorDaoDescriptor descriptor) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor castedDescriptor = castDescriptor(descriptor);
        LinkedList<ComtorJDBCField> fields = castedDescriptor.getInsertableFields();
        String sql = getInsertQuery(castedDescriptor, fields);

        if (LOG_EXECUTE_QUERY) {
            System.out.println("SQL >> " + sql);
        }

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(sql)) {
            this.preInsert(element, castedDescriptor);

            assignFieldsPreparedStatement(1, fields, element, ps);

            ps.execute();

            this.postInsert(element, castedDescriptor);
        } catch (SQLException ex) {
            throw new ComtorDaoException("Fail on insertElement: " + ex.getMessage(), ex);
        }
    }

    /**
     *
     * @param object
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void preInsert(Object object, ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        String sequenceQuery = descriptor.getSequenceQuery();

        if ((sequenceQuery != null) && (descriptor.getSequenceTypeInsert() == ComtorJDBCDaoDescriptor.SEQUENCE_PRE_INSERT)) {
            setIdFromSequence(object, descriptor);
        }
    }

    /*
     * 
     * @param object
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void postInsert(Object object, ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        String sequenceQuery = descriptor.getSequenceQuery();

        if ((sequenceQuery != null) && (descriptor.getSequenceTypeInsert() == ComtorJDBCDaoDescriptor.SEQUENCE_POST_INSERT)) {
            setIdFromSequence(object, descriptor);
        }
    }

    /**
     * asigna el valor del id consultado la secuencia.
     *
     * @param object
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    private void setIdFromSequence(Object object, ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        Class clazz = object.getClass();
        Field[] fields = ReflectionHelper.getAllFields(clazz);

        for (Field field : fields) {
            Annotation[] annotations = field.getAnnotations();

            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(ComtorId.class)) {
                    String keyName = field.getName();
                    String keyMethodName = "set" + keyName.substring(0, 1).toUpperCase() + keyName.substring(1, keyName.length());
                    long nextId = this.getNextId(descriptor);

                    if (nextId != 0) {
                        try {
                            Method methodSet = clazz.getMethod(keyMethodName, long.class);
                            methodSet.invoke(object, nextId);
                        } catch (Exception ex) {
                            throw new ComtorDaoException(ex);
                        }
                    }
                }
            }
        }
    }

    /**
     *
     * @param element
     * @param comtorDaoKey
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void updateElement(Object element, ComtorDaoKey comtorDaoKey, ComtorDaoDescriptor descriptor) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor castedDescriptor = castDescriptor(descriptor);
        LinkedList<ComtorJDBCField> updatableFields = castedDescriptor.getUpdatebleFields();
        String sql = getUpdateQuery(comtorDaoKey, castedDescriptor, updatableFields);

        if (LOG_EXECUTE_QUERY) {
            System.out.println("SQL >> " + sql);
        }

        int pos = 1;

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(sql)) {
            LinkedList<ComtorJDBCField> updatableFieldsWithoutKeys = new LinkedList<>();

            for (ComtorJDBCField field : updatableFields) {
                if (!comtorDaoKey.getKeys().containsKey(field.getAttributeName())) {
                    updatableFieldsWithoutKeys.add(field);
                }
            }

            pos = assignFieldsPreparedStatement(pos, updatableFieldsWithoutKeys, element, ps);

            LinkedList<ComtorJDBCField> whereKeys = new LinkedList<>();

            for (String key : comtorDaoKey.getKeys().keySet()) {
                ComtorJDBCField field = castedDescriptor.getField(key);
                whereKeys.add(field);
            }

            assignFieldsPreparedStatement(pos, whereKeys, element, ps);

            ps.execute();
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("ComtorJDBCDao " + ex.getMessage() + " SQL [" + sql + "]", ex);
        }
    }

    /**
     *
     * @param comtorDaoKey
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void deleteElement(ComtorDaoKey comtorDaoKey, ComtorDaoDescriptor descriptor) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor castedDescriptor = castDescriptor(descriptor);
        String sql = getDeleteQuery(comtorDaoKey, castedDescriptor);

        if (LOG_EXECUTE_QUERY) {
            System.out.println("SQL >> " + sql);
        }

        int pos = 1;

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(sql)) {
            for (String key : comtorDaoKey.getKeys().keySet()) {
                ComtorJDBCField field = castedDescriptor.getField(key);
                assignValueInPreparedStatement(ps, pos, field.getType(), comtorDaoKey.getValue(key));
                pos++;
            }

            ps.execute();
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Delete element failure", ex);
        }
    }

    /**
     *
     * @param comtorDaoKey
     * @param descriptor
     * @return
     */
    private String getDeleteQuery(ComtorDaoKey comtorDaoKey, ComtorJDBCDaoDescriptor descriptor) {
        String wherePart = "";
        int count = 0;

        for (String key : comtorDaoKey.getKeys().keySet()) {
            ComtorJDBCField field = descriptor.getField(key);
            wherePart += " AND  " + field.getColumnName() + " = ? ";
            count++;
        }

        if (count > 0) {
            wherePart = wherePart.substring(5);
        }

        return "DELETE FROM " + descriptor.getTableName() + ((count > 0) ? " WHERE " + wherePart : "");
    }

    /**
     *
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    private ComtorJDBCDaoDescriptor castDescriptor(ComtorDaoDescriptor desc) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor desc2;

        try {
            desc2 = (ComtorJDBCDaoDescriptor) desc;

            return desc2;
        } catch (ClassCastException exception) {
            throw new ComtorDaoException("Invalid ComtorDaoDescriptor", exception);
        }
    }

    /**
     *
     * @throws java.lang.Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        close();

        super.finalize();
    }

    /**
     *
     * @return
     */
    public java.sql.Connection getJdbcConnection() {
        return jdbcConnection;
    }

    public void beginTransaction() throws ComtorDaoException {
        try {
            jdbcConnection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new ComtorDaoException(ex);
        }
    }

    public void commit() throws ComtorDaoException {
        try {
            jdbcConnection.commit();
            jdbcConnection.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new ComtorDaoException(ex);
        }
    }

    public static ComtorDaoData executeQuery(ComtorJDBCDao dao, String query) throws ComtorDaoException {
        return executeQuery(dao, query, 0, -1);
    }

    public static ComtorDaoData executeQueryWithParams(ComtorJDBCDao dao, String query, long firstResult, long maxResults, Object... params) throws ComtorDaoException {
        return executeQueryWithParams(dao, query, false, firstResult, maxResults, params);
    }

    public static ComtorDaoData executeQueryWithParams(ComtorJDBCDao dao, String query, boolean withLabels, long firstResult, long maxResults, Object... params)
            throws ComtorDaoException {
        try (PreparedStatement ps = dao.getJdbcConnection().prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            int paramCount = 1;

            if (params != null) {
                for (Object param : params) {
                    Class classType = (param == null) ? Object.class : param.getClass();

                    assignValueInPreparedStatement(ps, paramCount, classType, param);

                    paramCount++;
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                ComtorDaoData data = new ComtorDaoData();
                rs.beforeFirst();

                if ((firstResult != 0) && !rs.absolute((int) firstResult)) {
                    return data;
                }

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String name = withLabels ? metaData.getColumnLabel(i) : metaData.getColumnName(i);
                    data.addHeader(name);

                    String type = metaData.getColumnTypeName(i);
                    data.addType(type);
                }

                long count = 0;

                while (rs.next() && ((count < maxResults) || (maxResults < 0))) {
                    LinkedList<Object> row = new LinkedList<>();

                    for (int i = 1; i <= columnCount; i++) {
                        try {
                            Object object = rs.getObject(i);
                            row.add(object);
                        } catch (SQLException ex) {
                            row.add(null);
                        }
                    }

                    data.addRow(row);
                    count++;
                }

                return data;
            }
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("[Execute Query] = " + query, ex);
        }
    }

    /**
     *
     * @param dao
     * @param query
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static ComtorDaoData executeQuery(ComtorJDBCDao dao, String query, long firstResult, long maxResults) throws ComtorDaoException {
        try (Statement stmt = dao.getJdbcConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                ComtorDaoData data = new ComtorDaoData();
                rs.beforeFirst();

                if ((firstResult != 0) && !rs.absolute((int) firstResult)) {
                    return data;
                }

                long count = 0;
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String name = metaData.getColumnName(i);
                    data.addHeader(name);
                    String type = metaData.getColumnTypeName(i);
                    data.addType(type);
                }

                while (rs.next() && ((count < maxResults) || (maxResults < 0))) {
                    LinkedList<Object> row = new LinkedList<>();

                    for (int i = 1; i <= columnCount; i++) {
                        try {
                            Object object = rs.getObject(i);
                            row.add(object);
                        } catch (SQLException ex) {
                            row.add(null);
                        }
                    }

                    data.addRow(row);
                    count++;
                }

                return data;
            }
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("[Execute Query] = " + query, ex);
        }
    }

    /**
     *
     * @param dao
     * @param sql
     * @param params
     * @return
     * @throws ComtorDaoException
     */
    public static Object executeQueryUniqueResult(ComtorJDBCDao dao, String sql, Object... params) throws ComtorDaoException {
        try (Connection conn = dao.getJdbcConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int paramCount = 1;

                for (Object param : params) {
                    if (param == null) {
                        assignValueInPreparedStatement(ps, paramCount, Object.class, param);
                    } else {
                        assignValueInPreparedStatement(ps, paramCount, param.getClass(), param);
                    }

                    paramCount++;
                }

                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getObject(1) : null;
                }
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException(ex);
        }
    }

    /**
     *
     * @param dao
     * @param sql
     * @param params
     * @return
     * @throws ComtorDaoException
     */
    public static int executeUpdate(ComtorJDBCDao dao, String sql, Object... params) throws ComtorDaoException {
        try (Connection conn = dao.getJdbcConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int paramCount = 1;

                for (Object param : params) {
                    assignValueInPreparedStatement(ps, paramCount, param.getClass(), param);
                    paramCount++;
                }

                return ps.executeUpdate();
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException(ex);
        }
    }

    /**
     *
     * @param obj
     * @param key
     * @param desc
     * @param fields
     * @return
     */
    private static String getUpdateQuery(ComtorDaoKey key, ComtorJDBCDaoDescriptor desc, LinkedList<ComtorJDBCField> fields) {
        StringBuffer setPart = new StringBuffer();
        int count = 0;

        for (ComtorJDBCField field : fields) {
            if (!key.getKeys().containsKey(field.getAttributeName())) {
                setPart.append(" , ");
                setPart.append(field.getColumnName());
                setPart.append(" = ? ");
                count++;
            }
        }

        if (count > 0) {
            setPart = new StringBuffer(setPart.toString().substring(2));
        }

        StringBuffer wherePart = new StringBuffer();
        count = 0;

        for (String k : key.getKeys().keySet()) {
            ComtorJDBCField field = desc.getField(k);
            wherePart.append(" AND ");
            wherePart.append(field.getColumnName());
            wherePart.append(" = ? ");
            count++;
        }

        if (count > 0) {
            wherePart = new StringBuffer(wherePart.toString().substring(5));
        }

        return "UPDATE " + desc.getTableName() + " SET " + setPart + " WHERE " + wherePart;
    }

    /**
     *
     * @param key
     * @param desc
     * @return
     */
    public static String getFindQuery(ComtorDaoKey key, ComtorJDBCDaoDescriptor desc) throws ComtorDaoException {
        String sql = getFindQuery(desc) + "  WHERE 1 = 1 ";
        String wherePart = "";
        int count = 0;

        for (String k : key.getKeys().keySet()) {
            ComtorJDBCField field = desc.getField(k);
            wherePart += " AND  " + desc.getTableName() + "." + field.getColumnName() + " = ? ";
            count++;
        }

        sql += wherePart;

        if (LOG_EXECUTE_QUERY) {
            System.out.println(">>> SQL: " + sql);
        }

        return sql;
    }

    /**
     *
     * @param descriptor
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static String getFindQuery(ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        String columnsSelect = getFindQueryFields(descriptor);
        String joins = getFindQueryJoins(descriptor);
        String tableNames = getFindQueryTables(descriptor);
        String sql = "SELECT " + columnsSelect + " FROM " + tableNames + joins;

        return sql;
    }

    /**
     *
     * @param descriptor
     * @return
     */
    public static String getFindQueryTables(ComtorJDBCDaoDescriptor descriptor) {
        return descriptor.getTableName() + " ";
    }

    /**
     * Retorna la parte de joins del query
     *
     * @param descriptor
     * @return
     */
    public static String getFindQueryJoins(ComtorJDBCDaoDescriptor descriptor) {
        StringBuilder leftJoin = new StringBuilder();
        LinkedList<String> assign = new LinkedList<>();

        for (ComtorJDBCJoin join : descriptor.getJoins()) {
            String tableName = "";

            if (join.getForeingClassDescriptor() != null) {
                tableName = join.getForeingClassDescriptor().getTableName();
            } else {
                tableName = join.getTableName();
            }

            String joinStr = " ";

            switch (join.getJoinType()) {
                case INNER:
                    joinStr = " INNER JOIN ";
                    break;
                case LEFT:
                    joinStr = " LEFT JOIN ";
                    break;
                case RIGHT:
                    joinStr = " RIGHT JOIN ";
                    break;
                default:
                    break;
            }

            if (descriptor.getDriver().equals(DRIVER_ORACLE) || descriptor.getDriver().equals(DRIVER_ORACLE_2)) {
                joinStr += " " + tableName + " " + join.getAlias();
            } else {
                joinStr += " " + tableName + " AS " + join.getAlias();
            }

            joinStr += " ON (" + join.onClause + " )";
            leftJoin.append(joinStr);
        }

        for (ComtorJDBCForeingField foreingField : descriptor.getForeingFields()) {
            String foreingFieldName = getForeingFieldName(foreingField, descriptor);

            if (!assign.contains(getForeingFieldId(foreingField))) {
                if (descriptor.getDriver().equals(DRIVER_ORACLE) || descriptor.getDriver().equals(DRIVER_ORACLE_2)) {
                    leftJoin.append(" LEFT JOIN " + foreingField.getDescriptor().getTableName() + " " + foreingFieldName);
                } else {
                    leftJoin.append(" LEFT JOIN " + foreingField.getDescriptor().getTableName() + " AS " + foreingFieldName);
                }

                String on = "";
                LinkedList<ComtorJDBCField> findableFields = foreingField.getDescriptor().getFindFields();

                if (findableFields.size() == foreingField.getReferencesColumn().length) {
                    for (int i = 0; i < findableFields.size(); i++) {
                        ComtorJDBCField findableField = findableFields.get(i);
                        on = on + " AND " + foreingFieldName + "." + findableField.getColumnName() + " = " + descriptor.getTableName()
                                + "." + foreingField.getReferencesColumn()[i];
                    }
                }

                on = (on.startsWith(" AND ")) ? on.substring(5, on.length()) : on;
                leftJoin.append(" ON (" + on + ") ");
                assign.add(getForeingFieldId(foreingField));
            }
        }

        return leftJoin.toString();
    }

    /**
     * Retorna la parte de campos que van a hacer seleccionados ejm: tabla.campo1, tabla.
     *
     *
     * @param descriptor
     * @return
     */
    public static String getFindQueryFields(ComtorJDBCDaoDescriptor descriptor) {
        String columnsSelect = "";
        LinkedList<ComtorJDBCField> selectableFields = descriptor.getSelectableFields();
        LinkedList<ComtorJDBCForeingField> foreingFields = descriptor.getForeingFields();
        LinkedList<ComtorJDBCForeingFieldByJoin> foreingFieldsByJoin = descriptor.getForeingFieldsByJoin();

        for (ComtorJDBCField field : selectableFields) {
            boolean isField = true;

            for (ComtorJDBCForeingField foreingField : foreingFields) {
                if (foreingField.getAttributeName().equals(field.getColumnName())) {
                    isField = false;
                    break;
                }
            }

            for (ComtorJDBCForeingFieldByJoin foreingField : foreingFieldsByJoin) {
                if (foreingField.getAttributeName().equals(field.getColumnName())) {
                    isField = false;
                    break;
                }
            }

            if (isField) {
                columnsSelect += descriptor.getTableName() + "." + field.getColumnName() + ", ";
            }
        }

        columnsSelect = columnsSelect.endsWith(", ") ? columnsSelect.substring(0, columnsSelect.length() - 2) : columnsSelect;

        for (ComtorJDBCForeingField foreingField : foreingFields) {
            String foreingFieldName = getForeingFieldName(foreingField, descriptor);
            columnsSelect = columnsSelect + ", " + foreingFieldName + "." + foreingField.getForeingColumn() + " AS " + foreingField.getAttributeName();
        }

        for (ComtorJDBCForeingFieldByJoin field : foreingFieldsByJoin) {
            ComtorJDBCJoin join = descriptor.getJoin(field.getJoinAlias());

            if (join.getForeingClassDescriptor() != null) {
                descriptor = join.getForeingClassDescriptor();
                columnsSelect += ", " + join.getAlias() + "." + descriptor.getField(field.getForeingFieldName()).getColumnName() + " AS " + field.getAttributeName();
            } else {
                columnsSelect += ", " + join.getAlias() + "." + field.getForeingFieldName() + " AS " + field.getAttributeName();
            }
        }

        return columnsSelect;
    }

    /**
     *
     * @param foreingField
     * @return
     */
    private static String getForeingFieldId(ComtorJDBCForeingField foreingField) {
        String id = foreingField.getDescriptor().getTableName() + "[";

        for (String field : foreingField.getReferencesColumn()) {
            id += field + ",";
        }

        return id + "]";
    }

    /**
     * Retorna el nombre de la tabla foranea mapeada
     *
     * @param foreingField
     * @param descriptor
     * @return
     */
    private synchronized static String getForeingFieldName(ComtorJDBCForeingField foreingField, ComtorJDBCDaoDescriptor descriptor) {
        HashMap<ComtorJDBCForeingField, String> map = new HashMap<>();
        int count = 1;

        for (ComtorJDBCForeingField jDBCForeingField : descriptor.getForeingFields()) {
            String tableName = jDBCForeingField.getDescriptor().getTableName();

            if (foreingField.getDescriptor().getTableName().equals(tableName)) {
                if (map.keySet().isEmpty()) {
                    map.put(jDBCForeingField, tableName);
                } else {
                    String tempId = getForeingFieldId(jDBCForeingField);
                    boolean flag = false;
                    CopyOnWriteArraySet<ComtorJDBCForeingField> list = new CopyOnWriteArraySet<>(map.keySet());

                    for (ComtorJDBCForeingField mapField : list) {
                        String maFieldId = getForeingFieldId(mapField);

                        if (tempId.equals(maFieldId)) {
                            map.put(jDBCForeingField, map.get(mapField));
                            flag = true;
                        }
                    }

                    if (!flag) {
                        map.put(jDBCForeingField, tableName + "_" + count);
                        count++;
                    }
                }
            }
        }

        return map.get(foreingField);
    }

    /**
     *
     * @param desc
     * @param fields
     * @return
     */
    public static String getInsertQuery(ComtorJDBCDaoDescriptor desc, LinkedList<ComtorJDBCField> fields) {
        StringBuilder tablePart = new StringBuilder();
        StringBuilder valuePart = new StringBuilder();

        for (ComtorJDBCField field : fields) {
            tablePart.append(", ").append(field.getColumnName()).append(" ");
            valuePart.append(", ? ");
        }

        String strTablePart = null;
        String strValuePart = null;

        if (fields.size() > 0) {
            strTablePart = tablePart.substring(1);
            strValuePart = valuePart.substring(1);
        } else {
            strTablePart = tablePart.toString();
            strValuePart = valuePart.toString();
        }

        return "INSERT INTO " + desc.getTableName() + " (" + strTablePart + ") VALUES (" + strValuePart + ") ";
    }

    /**
     *
     * @param descriptor
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public long getNextId(ComtorDaoDescriptor descriptor) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor castedDescriptor = castDescriptor(descriptor);
        String sql = castedDescriptor.getSequenceQuery();

        try (Statement stmt = getJdbcConnection().createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new ComtorDaoException("Bad sequence query " + sql);
                }

                return rs.getLong(1);
            }
        } catch (Exception ex) {
            throw new ComtorDaoException("Id Creation error", ex);
        }
    }

    /**
     *
     * @param queryString
     * @param descriptor
     * @param firstResult
     * @param maxResults
     * @param params
     * @return
     * @throws ComtorDaoException
     */
    @Override
    public LinkedList<Object> findAllRange(String queryString, ComtorDaoDescriptor descriptor, long firstResult, long maxResults, Object... params)
            throws ComtorDaoException {
        if (LOG_EXECUTE_QUERY) {
            System.out.println(">>> SQL: " + queryString);
        }

        ComtorJDBCDaoDescriptor castedDescriptor = (ComtorJDBCDaoDescriptor) descriptor;
        LinkedList<Object> resp = new LinkedList<>();

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(queryString, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
            int paramCount = 1;

            for (Object param : params) {
                if (param == null) {
                    assignValueInPreparedStatement(ps, paramCount, Object.class, null);
                } else {
                    assignValueInPreparedStatement(ps, paramCount, param.getClass(), param);
                }

                paramCount++;
            }

            try (ResultSet rs = ps.executeQuery()) {
                rs.beforeFirst();

                if ((firstResult != 0) && !rs.absolute((int) firstResult)) {
                    return resp;
                }

                long count = 0;

                while (rs.next() && ((count < maxResults) || (maxResults < 0))) {
                    Object obj = castedDescriptor.getObjectClass().newInstance();
                    fillObject(obj, rs, castedDescriptor);
                    resp.add(obj);

                    count++;
                }
            }
        } catch (Exception ex) {
            throw new ComtorDaoException("execute Query " + queryString, ex);
        }

        return resp;
    }

    public LinkedList<Object> findAllRangeSqlite(String queryString, ComtorDaoDescriptor desc, long firstResult, long maxResults, Object... params)
            throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = (ComtorJDBCDaoDescriptor) desc;
        LinkedList<Object> resp = new LinkedList<>();

        if (LOG_EXECUTE_QUERY) {
            System.out.println(">>> SQL: " + queryString);
        }

        try (PreparedStatement ps = getJdbcConnection().prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            int paramCount = 1;

            for (Object param : params) {
                if (param == null) {
                    assignValueInPreparedStatement(ps, paramCount, Object.class, null);
                } else {
                    assignValueInPreparedStatement(ps, paramCount, param.getClass(), param);
                }

                paramCount++;
            }

            try (ResultSet rs = ps.executeQuery()) {
                long count = 0;

                while (rs.next() && ((count < maxResults) || (maxResults < 0))) {
                    Object obj = descriptor.getObjectClass().newInstance();
                    fillObject(obj, rs, descriptor);
                    resp.add(obj);

                    count++;
                }
            }
        } catch (Exception ex) {
            throw new ComtorDaoException("execute Query " + queryString, ex);
        }

        return resp;
    }

    /**
     *
     * @param query
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public LinkedList<Object> findAll(String query, ComtorDaoDescriptor desc, Object... params) throws ComtorDaoException {
        return findAllRange(query, desc, 0, -1, params);
    }

    /**
     * Finds all objects by key.
     *
     * @param key COMTOR key.
     * @param descriptor COMTOR JDBC descriptor.
     * @return List of object that have the specified key's values.
     * @throws ComtorDaoException
     */
    public LinkedList<Object> findAll(ComtorDaoKey key, ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        String sql = getFindQuery(key, descriptor);
        Object[] values = key.getKeys().values().toArray();

        return findAll(sql, descriptor, values);
    }

    /**
     *
     * @param sql
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static boolean execute(ComtorJDBCDao dao, String sql) throws ComtorDaoException {
        try (Statement stmt = dao.getJdbcConnection().createStatement()) {
            stmt.execute(sql);

            return true;
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("execute Query " + sql, ex);
        }
    }

    /**
     *
     * @param dao
     * @param queryString
     * @param params
     * @return
     * @throws ComtorDaoException
     */
    public static boolean execute(ComtorJDBCDao dao, String queryString, Object... params) throws ComtorDaoException {
        try (PreparedStatement ps = dao.getJdbcConnection().prepareStatement(queryString)) {
            int paramCount = 1;

            for (Object param : params) {
                if (param == null) {
                    assignValueInPreparedStatement(ps, paramCount, Object.class, param);
                } else {
                    assignValueInPreparedStatement(ps, paramCount, param.getClass(), param);
                }

                paramCount++;
            }

            return ps.execute();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("execute Query " + queryString, ex);
        }
    }

    /**
     *
     * @param dao
     * @param queryString
     * @param params
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static long countResultsQuery(ComtorJDBCDao dao, String queryString, Object... params) throws ComtorDaoException {
        String sql = "SELECT COUNT(1) FROM (" + queryString + ") ";

        if (!dao.driver.equals(DRIVER_ORACLE) || dao.driver.equals(DRIVER_ORACLE_2)) {
            //TODO: Arreglar el lio  del AS
            //sql += " AS  QTY ";
            sql += "   QTY ";
        }

        try (PreparedStatement ps = dao.getJdbcConnection().prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            int paramCount = 1;

            for (Object object : params) {
                if (object == null) {
                    assignValueInPreparedStatement(ps, paramCount, Object.class, object);
                } else {
                    assignValueInPreparedStatement(ps, paramCount, object.getClass(), object);
                }

                paramCount++;
            }

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Query : " + queryString + " -> " + ex.getMessage());
        }
    }

    // TODO: QUITAR
    @Deprecated
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

    private String buildPreparedStatementWildcards(int numWildcards) {
        StringBuilder sb = new StringBuilder(numWildcards * 2);

        for (int i = 0; i < numWildcards; i++) {
            sb.append("?").append((i < numWildcards - 1) ? "," : "");
        }

        return sb.toString();
    }

    private int assignFieldsPreparedStatement(int firstPosition, LinkedList<ComtorJDBCField> fields, Object element, PreparedStatement ps) throws SQLException {
        int position = firstPosition;

        for (int i = 0; i < fields.size(); i++) {
            ComtorJDBCField f = fields.get(i);
            Class type = f.getType();
            Object value = f.getValue(element);
            assignValueInPreparedStatement(ps, position++, type, value);
        }

        return position;
    }
}
