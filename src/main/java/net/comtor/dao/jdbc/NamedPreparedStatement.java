package net.comtor.dao.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jorgegarcia@comtor.net
 * @since Apr 4, 2014
 * @version
 */
public class NamedPreparedStatement {

    public static boolean WITHLOG  = Boolean.FALSE;
    
    private String namedSql;
    private Map<String, Object> namedMap;

    public NamedPreparedStatement(String namedSql, Map<String, Object> namedMap) {
        this.namedSql = namedSql;
        this.namedMap = namedMap;
    }

    public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
        List<String> namedParameters = findNamedParameters();
        String sql = replaceNamedParametersByWildcards();
        if(WITHLOG){
            System.out.println("ReplacedSql = " + sql);
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        for (int index = 0; index < namedParameters.size(); index++) {
            String namedParameter = namedParameters.get(index);
            Object value = namedMap.get(namedParameter);
            setValue(ps, index + 1, value);
        }

        return ps;
    }

    private List<String> findNamedParameters() {
        String[] parametersIndexed = new String[namedSql.length()];

        Set<String> namedParameters = namedMap.keySet();
        for (String namedParameter : namedParameters) {
            int index = 0;
            while (index != -1) {
                index = namedSql.indexOf(namedParameter, index + 1);
                if (index != -1) {
                    parametersIndexed[index] = namedParameter;
                }
            }
        }

        List<String> list = new LinkedList<String>();

        for (String parameterIndexed : parametersIndexed) {
            if (parameterIndexed != null) {
                list.add(parameterIndexed);
            }
        }

        return list;
    }

    private String replaceNamedParametersByWildcards() {
        String sql = namedSql;

        Set<String> namedParameters = namedMap.keySet();
        for (String namedParameter : namedParameters) {
            sql = sql.replaceAll(namedParameter, "?");
        }

        return sql;
    }

    private void setValue(PreparedStatement ps, int parameterIndex, Object value) throws SQLException {
        Class<? extends Object> valueClass = value.getClass();
        if (valueClass.equals(BigDecimal.class)) {
            ps.setBigDecimal(parameterIndex, (BigDecimal) value);
            return;
        }

        if (valueClass.equals(boolean.class) || valueClass.equals(Boolean.class)) {
            ps.setBoolean(parameterIndex, (Boolean) value);
            return;
        }
        if (valueClass.equals(byte.class) || valueClass.equals(Byte.class)) {
            ps.setByte(parameterIndex, (Byte) value);
            return;
        }

        if (valueClass.equals(Date.class)) {
            ps.setDate(parameterIndex, (Date) value);
            return;
        }

        if (valueClass.equals(double.class) || valueClass.equals(Double.class)) {
            ps.setDouble(parameterIndex, (Double) value);
            return;
        }

        if (valueClass.equals(float.class) || valueClass.equals(Float.class)) {
            ps.setFloat(parameterIndex, (Float) value);
            return;
        }

        if (valueClass.equals(int.class) || valueClass.equals(Integer.class)) {
            ps.setInt(parameterIndex, (Integer) value);
            return;
        }

        if (valueClass.equals(long.class) || valueClass.equals(Long.class)) {
            ps.setLong(parameterIndex, (Long) value);
            return;
        }

        if (valueClass.equals(short.class) || valueClass.equals(Short.class)) {
            ps.setShort(parameterIndex, (Short) value);
            return;
        }

        if (valueClass.equals(String.class)) {
            ps.setString(parameterIndex, (String) value);
            return;
        }

        if (valueClass.equals(Time.class)) {
            ps.setTime(parameterIndex, (Time) value);
            return;
        }

        if (valueClass.equals(Timestamp.class)) {
            ps.setTimestamp(parameterIndex, (Timestamp) value);
            return;
        }

        throw new SQLException("No supported value Class: " + valueClass.getCanonicalName());
    }
}
