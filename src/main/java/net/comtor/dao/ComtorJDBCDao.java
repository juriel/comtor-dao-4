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

    public static final String DRIVER_SQL_SERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DRIVER_POSTGRES = "org.postgresql.Driver";
    public static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
    public static final String DRIVER_ORACLE = "oracle.jdbc.OracleDriver";
    public static final String DRIVER_ORACLE_2 = "oracle.jdbc.driver.OracleDriver";
    public static final String DRIVER_SYBASE = "com.sybase.jdbc2.jdbc.SybDriver";
    public static final String DRIVER_SQLITE = "org.sqlite.JDBC";

    public static final String MYSQL_SEQUENCE = "SELECT LAST_INSERT_ID()";
    public static final String SQL_SERVER_SEQUENCE = "SELECT @@IDENTITY";
    public static final String SQLITE_SEQUENCE = "SELECT last_insert_rowid()";

    private java.sql.Connection jdbcConnection = null;
    String driver;
    String url;
    String user;
    String password;

    public ComtorJDBCDao(Connection conn) throws ComtorDaoException {
        jdbcConnection = conn;
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
            throw new ComtorJDBCDaoConstructionFailedException("ComtorJDBCDao "
                    + "construction failed " + ex.getMessage(), ex);
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
    public ComtorJDBCDao(String driver, String url, String user, String password)
            throws ComtorDaoException {
        super();

        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;

        try {
            initConnection(driver, url, user, password);
        } catch (Exception ex) {
            throw new ComtorJDBCDaoConstructionFailedException("ComtorJDBCDao "
                    + "construction failed (" + url + "," + " " + user + " , xxxxx , " + ex, ex);
        }
    }

    public String getDriver() {
        return driver;
    }

    /**
     *
     * @param jdbc
     */
    protected void setJdbcConnection(java.sql.Connection jdbc) {
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
    protected void initConnection(String driver, String url, String user, String password)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);

        jdbcConnection = (user == null)
                ? DriverManager.getConnection(url)
                : DriverManager.getConnection(url, user, password);
    }

    /**
     * close jdbc Connnection
     */
    public void close() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }

            jdbcConnection = null;
        } catch (SQLException ex) {
        }
    }

    /**
     * finds element in database
     *
     * @param daoKey
     * @param desc
     * @return Object
     * @throws net.comtor.dao.ComtorDaoException
     */
    public Object findElement(ComtorDaoKey daoKey, ComtorDaoDescriptor desc) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = castDescriptor(desc);
        String pair = "";
        String query = getFindQuery(daoKey, descriptor);
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = getJdbcConnection().prepareStatement(query);
            int i = 0;

            for (String key : daoKey.getKeys().keySet()) {
                int pos = i + 1;

                ComtorJDBCField field = descriptor.getField(key);
                Object value = daoKey.getValue(field.getAttributeName());

                pair += field.getAttributeName() + " " + value;

                assignValueInPreparedStatement(ps, pos, field.getType(), value);

                i++;
            }

            rs = ps.executeQuery();

            if (rs.next()) {
                Object result = desc.getObjectClass().newInstance();
                fillObject(result, rs, descriptor);

                if (rs.next()) {
                    throw new ComtorDaoException("findElement founds more than "
                            + "one element. [" + query + "]" + pair);
                }

                return result;
            }

            return null;
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Can't create prepareStatement: " + query
                    + " " + ex.getMessage(), ex);
        } catch (InstantiationException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Can't Instantiate: " + ex.getMessage(), ex);
        } catch (IllegalAccessException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            safeClose(null, null, ps, null);
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

        for (ComtorJDBCField field : selectableFields) {
            Class fieldType = field.getType();
            String columnName = field.getColumnName();
            Object obj[] = new Object[1];  // parameter for invoke

            try {
                Object dataFromResultSet = rs.getObject(columnName);

                if (dataFromResultSet != null) {
                    obj[0] = dataFromResultSet;

                    try {
                        Class dataType = dataFromResultSet.getClass();

                        if ((fieldType.equals(long.class) || fieldType.equals(double.class))
                                && dataFromResultSet == null) {
                            field.getSetMethod().invoke(result, 0);
                        } else if (fieldType.equals(long.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).longValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(long.class) && dataType.equals(BigInteger.class)) {
                            obj[0] = ((BigInteger) dataFromResultSet).longValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Long.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).longValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Long.class) && dataType.equals(BigInteger.class)) {
                            obj[0] = ((BigInteger) dataFromResultSet).longValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(double.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).doubleValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Double.class) && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).doubleValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if ((fieldType.equals(float.class) || fieldType.equals(Float.class))
                                && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).floatValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if ((fieldType.equals(int.class) || fieldType.equals(Integer.class))
                                && dataType.equals(BigDecimal.class)) {
                            obj[0] = ((BigDecimal) dataFromResultSet).intValue();
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(long.class) && dataType.equals(Boolean.class)) {
                            if (((Boolean) dataFromResultSet) == true) {
                                obj[0] = 1;
                            } else if (((Boolean) dataFromResultSet) == false) {
                                obj[0] = 0;
                            } else {
                                obj[0] = -1;
                            }

                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Timestamp.class)) {
                            obj[0] = rs.getTimestamp(columnName);
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Time.class)) {
                            obj[0] = rs.getTime(columnName);
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(java.sql.Date.class)) {
                            obj[0] = rs.getDate(columnName);
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(String.class) && dataFromResultSet instanceof java.sql.Clob) {
                            obj[0] = rs.getString(columnName);
                            field.getSetMethod().invoke(result, obj);
                        } else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
                            obj[0] = rs.getBoolean(columnName);
                            field.getSetMethod().invoke(result, obj);
                        } else {
                            field.getSetMethod().invoke(result, obj);
                        }
                    } catch (Exception ex) {
                        System.err.println("ERROR FILL  table " + daoDescriptor.getTableName()
                                + " :  column " + field.getColumnName() + " ("
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
                LOG.log(Level.SEVERE, ex.getMessage(), ex);
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
    public static void assignValueInPreparedStatement(PreparedStatement ps, int pos,
            Class type, Object value) throws SQLException {
        try {
            if (value == null) {
                ps.setNull(pos, java.sql.Types.INTEGER);

                return;
            }

            if (type.equals(String.class)) {
                ps.setString(pos, (String) value);
            } else if (type.equals(short.class)) {
                ps.setShort(pos, ((Short) value));
            } else if (type.equals(int.class) && value instanceof java.lang.Integer) {
                ps.setInt(pos, ((Integer) value));
            } else if (type.equals(int.class) && value instanceof java.lang.Long) {
                ps.setLong(pos, ((Long) value));
            } else if (type.equals(long.class) && value instanceof java.lang.Long) {
                ps.setLong(pos, ((Long) value));
            } else if (type.equals(long.class) && value instanceof java.lang.Integer) {
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
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void insertElement(Object element, ComtorDaoDescriptor desc) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = castDescriptor(desc);
        LinkedList<ComtorJDBCField> fields = descriptor.getInsertableFields();
        String query = getInsertQuery(descriptor, fields);
        PreparedStatement ps = null;

        try {
            this.preInsert(element, descriptor);

            ps = getJdbcConnection().prepareStatement(query);

            assignFieldsPreparedStatement(1, fields, element, ps);

            ps.execute();

            this.postInsert(element, descriptor);
        } catch (SQLException ex) {
            throw new ComtorDaoException("Fail on insertElement: " + ex.getMessage(), ex);
        } finally {
            safeClose(null, null, ps, null);
        }

    }

    public static int assignFieldsPreparedStatement(int firstPosition,
            LinkedList<ComtorJDBCField> fields, Object element, PreparedStatement ps)
            throws SQLException {
        int position = firstPosition;

        for (ComtorJDBCField field : fields) {
            Class type = field.getType();
            Object value = field.getValue(element);

            assignValueInPreparedStatement(ps, position++, type, value);
        }

        return position;
    }

    /**
     *
     * @param object
     * @param descriptor
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void preInsert(Object object, ComtorJDBCDaoDescriptor descriptor) throws ComtorDaoException {
        String sequenceQuery = descriptor.getSequenceQuery();

        if ((sequenceQuery != null)
                && (descriptor.getSequenceTypeInsert() == ComtorJDBCDaoDescriptor.SEQUENCE_PRE_INSERT)) {
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

        if ((sequenceQuery != null)
                && (descriptor.getSequenceTypeInsert() == ComtorJDBCDaoDescriptor.SEQUENCE_POST_INSERT)) {
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
                    String keyMethodName = "set" + keyName.substring(0, 1).toUpperCase()
                            + keyName.substring(1, keyName.length());
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
     * @param daoKey
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void updateElement(Object element, ComtorDaoKey daoKey, ComtorDaoDescriptor desc)
            throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = castDescriptor(desc);
        LinkedList<ComtorJDBCField> updatableFields = descriptor.getUpdatebleFields();
        String query = getUpdateQuery(daoKey, descriptor, updatableFields);
        PreparedStatement ps = null;
        int pos = 1;

        try {
            ps = getJdbcConnection().prepareStatement(query);

            LinkedList<ComtorJDBCField> updatableFieldsWithoutKeys = new LinkedList<>();

            for (ComtorJDBCField field : updatableFields) {
                if (!daoKey.getKeys().containsKey(field.getAttributeName())) {
                    updatableFieldsWithoutKeys.add(field);
                }
            }

            pos = assignFieldsPreparedStatement(pos, updatableFieldsWithoutKeys, element, ps);

            LinkedList<ComtorJDBCField> whereKeys = new LinkedList<>();

            for (String key : daoKey.getKeys().keySet()) {
                ComtorJDBCField field = descriptor.getField(key);
                whereKeys.add(field);
            }

            assignFieldsPreparedStatement(pos, whereKeys, element, ps);

            ps.execute();
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("ComtorJDBCDao " + ex.getMessage() + " SQL [" + query + "]", ex);
        } finally {
            safeClose(null, null, ps, null);
        }
    }

    /**
     *
     * @param daoKey
     * @param desc
     * @throws net.comtor.dao.ComtorDaoException
     */
    public void deleteElement(ComtorDaoKey daoKey, ComtorDaoDescriptor desc) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = castDescriptor(desc);
        String query = getDeleteQuery(daoKey, descriptor);
        PreparedStatement ps = null;

        try {
            ps = getJdbcConnection().prepareStatement(query);

            int pos = 1;

            for (String key : daoKey.getKeys().keySet()) {
                ComtorJDBCField field = descriptor.getField(key);
                assignValueInPreparedStatement(ps, pos, field.getType(), daoKey.getValue(key));

                pos++;
            }

            ps.execute();
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Delete element failure", ex);
        } finally {
            safeClose(null, null, ps, null);
        }

    }

    /**
     *
     * @param daoKey
     * @param desc2
     * @return
     */
    private String getDeleteQuery(ComtorDaoKey daoKey, ComtorJDBCDaoDescriptor desc) {
        String wherePart = "";
        int count = 0;

        for (String key : daoKey.getKeys().keySet()) {
            ComtorJDBCField field = desc.getField(key);
            wherePart += " AND  " + field.getColumnName() + " = ? ";

            count++;
        }

        if (count > 0) {
            wherePart = wherePart.substring(5);
        }

        return "DELETE FROM " + desc.getTableName()
                + ((count > 0)
                        ? " WHERE " + wherePart
                        : "");
    }

    /**
     *
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    private ComtorJDBCDaoDescriptor castDescriptor(ComtorDaoDescriptor desc)
            throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor;

        try {
            descriptor = (ComtorJDBCDaoDescriptor) desc;

            return descriptor;
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

    public static ComtorDaoData executeQuery(ComtorJDBCDao dao, String query)
            throws ComtorDaoException {
        return executeQuery(dao, query, 0, -1);
    }

    public static ComtorDaoData executeQueryWithParams(ComtorJDBCDao dao,
            String query, long firstResult, long maxResults, Object... params)
            throws ComtorDaoException {
        return executeQueryWithParams(dao, query, false, firstResult, maxResults, params);
    }

    public static ComtorDaoData executeQueryWithParams(ComtorJDBCDao dao, String query,
            boolean withLabels, long firstResult, long maxResults, Object... params)
            throws ComtorDaoException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = dao.getJdbcConnection().prepareStatement(query,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            if ((params != null) && (params.length > 0)) {
                int pos = 1;

                for (Object object : params) {
                    Class classType = (object == null) ? Object.class : object.getClass();

                    assignValueInPreparedStatement(ps, pos, classType, object);

                    pos++;
                }
            }

            rs = ps.executeQuery();

            ComtorDaoData data = new ComtorDaoData();

            rs.beforeFirst();

            if ((firstResult != 0) && !rs.absolute((int) firstResult)) {
                return data;
            }

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String name = withLabels
                        ? metaData.getColumnLabel(i)
                        : metaData.getColumnName(i);
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
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("[Execute Query] = " + query, ex);
        } finally {
            safeClose(null, null, ps, rs);
        }
    }

    /**
     *
     * @param dao
     * @param query
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static ComtorDaoData executeQuery(ComtorJDBCDao dao, String query,
            long firstResult, long maxResults) throws ComtorDaoException {
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = dao.getJdbcConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(query);

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
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("[Execute Query] = " + query, ex);
        } finally {
            safeClose(null, null, stmt, rs);
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
    public static Object executeQueryUniqueResult(ComtorJDBCDao dao, String sql,
            Object... params) throws ComtorDaoException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = dao.getJdbcConnection();
            ps = conn.prepareStatement(sql);
            int pos = 1;

            for (Object param : params) {
                Class classType = (param == null) ? Object.class : param.getClass();

                assignValueInPreparedStatement(ps, pos, classType, param);

                pos++;
            }

            rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getObject(1);
            }

            return null;
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException(ex);
        } finally {
            safeClose(null, null, ps, rs);
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
    public static int executeUpdate(ComtorJDBCDao dao, String sql, Object... params)
            throws ComtorDaoException {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = dao.getJdbcConnection();
            ps = conn.prepareStatement(sql);
            int pos = 1;

            for (Object param : params) {
                assignValueInPreparedStatement(ps, pos, param.getClass(), param);
                pos++;
            }

            return ps.executeUpdate();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException(ex);
        } finally {
            safeClose(null, null, ps, null);
        }
    }

    /**
     *
     * @param obj
     * @param daoKey
     * @param desc
     * @param fields
     * @return
     */
    private static String getUpdateQuery(ComtorDaoKey daoKey, ComtorJDBCDaoDescriptor desc,
            LinkedList<ComtorJDBCField> fields) {
        StringBuffer setPart = new StringBuffer();
        int count = 0;

        for (ComtorJDBCField field : fields) {
            if (!daoKey.getKeys().containsKey(field.getAttributeName())) {
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

        for (String key : daoKey.getKeys().keySet()) {
            ComtorJDBCField field = desc.getField(key);
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
     * @param daoKey
     * @param desc
     * @return
     */
    public static String getFindQuery(ComtorDaoKey daoKey, ComtorJDBCDaoDescriptor desc)
            throws ComtorDaoException {
        String query = getFindQuery(desc) + "  WHERE 1 = 1 ";
        String wherePart = "";

        for (String key : daoKey.getKeys().keySet()) {
            ComtorJDBCField field = desc.getField(key);
            wherePart += " AND  " + desc.getTableName() + "." + field.getColumnName() + " = ? ";
        }

        return query += wherePart;
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

        return "SELECT " + columnsSelect + " FROM " + tableNames + joins;
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
            ComtorJDBCDaoDescriptor foreingClassDescriptor = join.getForeingClassDescriptor();

            String tableName = (foreingClassDescriptor == null)
                    ? join.getTableName()
                    : foreingClassDescriptor.getTableName();

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

                        on = on + " AND " + foreingFieldName + "." + findableField.getColumnName()
                                + " = " + descriptor.getTableName()
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
     * Retorna la parte de campos que van a hacer seleccionados ejm:
     * tabla.campo1, tabla.
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

        columnsSelect = columnsSelect.endsWith(", ")
                ? columnsSelect.substring(0, columnsSelect.length() - 2)
                : columnsSelect;

        for (ComtorJDBCForeingField foreingField : foreingFields) {
            String foreingFieldName = getForeingFieldName(foreingField, descriptor);
            columnsSelect = columnsSelect + ", " + foreingFieldName
                    + "." + foreingField.getForeingColumn()
                    + " AS " + foreingField.getAttributeName();
        }

        for (ComtorJDBCForeingFieldByJoin field : foreingFieldsByJoin) {
            ComtorJDBCJoin join = descriptor.getJoin(field.getJoinAlias());

            if (join.getForeingClassDescriptor() == null) {
                columnsSelect += ", " + join.getAlias()
                        + "." + field.getForeingFieldName()
                        + " AS " + field.getAttributeName();
            } else {
                descriptor = join.getForeingClassDescriptor();
                columnsSelect += ", " + join.getAlias() + "."
                        + descriptor.getField(field.getForeingFieldName()).getColumnName()
                        + " AS " + field.getAttributeName();
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
    private synchronized static String getForeingFieldName(ComtorJDBCForeingField foreingField,
            ComtorJDBCDaoDescriptor descriptor) {
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

        if (fields.isEmpty()) {
            strTablePart = tablePart.toString();
            strValuePart = valuePart.toString();
        } else {
            strTablePart = tablePart.substring(1);
            strValuePart = valuePart.substring(1);
        }

        return ""
                + "INSERT INTO "
                + desc.getTableName() + " (" + strTablePart + ") "
                + "VALUES (" + strValuePart + ") ";
    }

    /**
     *
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public long getNextId(ComtorDaoDescriptor desc) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor daoDescriptor = castDescriptor(desc);
        String query = daoDescriptor.getSequenceQuery();
        ResultSet rs = null;
        Statement stmt = null;

        try {
            stmt = getJdbcConnection().createStatement();
            rs = stmt.executeQuery(query);

            if (rs.next()) {
                return rs.getLong(1);
            }

            throw new ComtorDaoException("Bad sequence query " + query);
        } catch (Exception ex) {
            throw new ComtorDaoException("Id Creation error", ex);
        } finally {
            safeClose(null, null, stmt, rs);
        }
    }

    /**
     *
     * @param queryString
     * @param desc
     * @param firstResult
     * @param maxResults
     * @param params
     * @return
     * @throws ComtorDaoException
     */
    @Override
    public LinkedList<Object> findAllRange(String queryString, ComtorDaoDescriptor desc,
            long firstResult, long maxResults, Object... params) throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = (ComtorJDBCDaoDescriptor) desc;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            LinkedList<Object> resp = new LinkedList<>();
            ps = getJdbcConnection().prepareStatement(queryString,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            int pos = 1;

            for (Object param : params) {
                Class classType = (param == null) ? Object.class : param.getClass();
                Object value = (param == null) ? null : param;

                assignValueInPreparedStatement(ps, pos, classType, value);

                pos++;
            }

            rs = ps.executeQuery();
            rs.beforeFirst();

            if ((firstResult != 0) && !rs.absolute((int) firstResult)) {
                return resp;
            }

            long count = 0;

            while (rs.next() && (count < maxResults || maxResults < 0)) {
                Object obj = descriptor.getObjectClass().newInstance();
                fillObject(obj, rs, descriptor);
                resp.add(obj);
                count++;
            }

            return resp;
        } catch (SQLException | InstantiationException | IllegalAccessException ex) {
            throw new ComtorDaoException("execute Query " + queryString, ex);
        } finally {
            safeClose(null, null, ps, rs);
        }
    }

    public LinkedList<Object> findAllRangeSqlite(String queryString, ComtorDaoDescriptor desc,
            long firstResult, long maxResults, Object... parameters)
            throws ComtorDaoException {
        ComtorJDBCDaoDescriptor descriptor = (ComtorJDBCDaoDescriptor) desc;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            LinkedList<Object> resp = new LinkedList<>();
            ps = getJdbcConnection().prepareStatement(queryString,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            int pos = 1;

            for (Object param : parameters) {
                Class classType = (param == null) ? Object.class : param.getClass();
                Object value = (param == null) ? null : param;

                assignValueInPreparedStatement(ps, pos, classType, value);

                pos++;
            }

            rs = ps.executeQuery();
            long count = 0;

            while (rs.next() && ((count < maxResults) || (maxResults < 0))) {
                Object obj = descriptor.getObjectClass().newInstance();
                fillObject(obj, rs, descriptor);
                resp.add(obj);
                count++;
            }

            return resp;
        } catch (SQLException | InstantiationException | IllegalAccessException ex) {
            throw new ComtorDaoException("execute Query " + queryString, ex);
        } finally {
            safeClose(null, null, ps, rs);
        }
    }

    /**
     *
     * @param query
     * @param desc
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public LinkedList<Object> findAll(String query, ComtorDaoDescriptor desc, Object... params)
            throws ComtorDaoException {
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
    public LinkedList<Object> findAll(ComtorDaoKey key, ComtorJDBCDaoDescriptor descriptor)
            throws ComtorDaoException {
        String query = getFindQuery(key, descriptor);
        Object[] values = key.getKeys().values().toArray();

        return findAll(query, descriptor, values);
    }

    /**
     *
     * @param queryString
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    public static boolean execute(ComtorJDBCDao dao, String queryString) throws ComtorDaoException {
        Statement stmt = null;

        try {
            stmt = dao.getJdbcConnection().createStatement();
            stmt.execute(queryString);

            return true;
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("execute Query " + queryString, ex);
        } finally {
            safeClose(null, null, stmt, null);
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
    public static boolean execute(ComtorJDBCDao dao, String queryString, Object... params)
            throws ComtorDaoException {
        PreparedStatement ps = null;

        try {
            ps = dao.getJdbcConnection().prepareStatement(queryString);
            int pos = 1;

            for (Object param : params) {
                Class classType = (param == null) ? Object.class : param.getClass();

                assignValueInPreparedStatement(ps, pos, classType, param);

                pos++;
            }

            return ps.execute();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("execute Query " + queryString, ex);
        } finally {
            safeClose(null, null, ps, null);
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
    public static long countResultsQuery(ComtorJDBCDao dao, String queryString, Object... params)
            throws ComtorDaoException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String query = "SELECT COUNT(1) FROM (" + queryString + ") ";

            if (!dao.driver.equals(DRIVER_ORACLE) || dao.driver.equals(DRIVER_ORACLE_2)) {
                query += "   QTY ";
            }

            ps = dao.getJdbcConnection().prepareStatement(query,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            int pos = 1;

            for (Object param : params) {
                Class classType = (param == null) ? Object.class : param.getClass();

                assignValueInPreparedStatement(ps, pos, classType, param);

                pos++;
            }

            rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getLong(1);
            }

            return -1;
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);

            throw new ComtorDaoException("Query : " + queryString + " -> " + ex.getMessage());
        } finally {
            safeClose(null, null, ps, rs);
        }
    }

    public static String buildPreparedStatementWildcards(int numWildcards) {
        StringBuilder sb = new StringBuilder(numWildcards * 2);

        for (int i = 0; i < numWildcards; i++) {
            sb.append("?").append((i < numWildcards - 1) ? "," : "");
        }

        return sb.toString();
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
