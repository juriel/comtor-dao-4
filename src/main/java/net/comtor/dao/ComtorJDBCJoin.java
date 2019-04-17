package net.comtor.dao;

/**
 *
 * @author juriel
 */
public class ComtorJDBCJoin {

    public enum JOIN_TYPE {
        LEFT, RIGHT, INNER
    }
    private String alias;
    private String tableName;
    Class referencesClass;
    JOIN_TYPE joinType;
    String onClause;
    private ComtorJDBCDaoDescriptor foreingClassDescriptor;


    public ComtorJDBCJoin(String alias, Class referencesClass, JOIN_TYPE joinType, String onClause) {
        this.alias = alias;
        this.tableName = null;
        this.referencesClass = referencesClass;
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public ComtorJDBCJoin(String alias, String tableName, JOIN_TYPE joinType, String onClause) {
        this.alias = alias;
        this.tableName = tableName;
        this.referencesClass = null;
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Class getReferencesClass() {
        return referencesClass;
    }

    public void setReferencesClass(Class referencesClass) {
        this.referencesClass = referencesClass;
    }

    public JOIN_TYPE getJoinType() {
        return joinType;
    }

    public void setJoinType(JOIN_TYPE joinType) {
        this.joinType = joinType;
    }

    public String getOnClause() {
        return onClause;
    }

    public void setOnClause(String onClause) {
        this.onClause = onClause;
    }

    public void setForeingClassDescriptor(ComtorJDBCDaoDescriptor foreingClassDescriptor) {
        this.foreingClassDescriptor = foreingClassDescriptor;
    }

    public ComtorJDBCDaoDescriptor getForeingClassDescriptor() {
        return foreingClassDescriptor;
    }

}
