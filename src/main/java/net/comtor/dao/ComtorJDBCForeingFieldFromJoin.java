package net.comtor.dao;

/**
 *
 * @author ericson
 */
public class ComtorJDBCForeingFieldFromJoin extends ComtorJDBCField {

    private String tableAlias;
    private String columnName;
    private String alias;

    public ComtorJDBCForeingFieldFromJoin(Class clazz, String fieldName, String tableName, boolean isboolean, String tableAlias, String columnName, String alias) {
        super(clazz, fieldName, tableName, isboolean);
        this.tableAlias = tableAlias;
        this.columnName = columnName;
        this.alias = alias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
