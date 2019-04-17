package net.comtor.dao;

/**
 *
 * @author juriel
 */
public class ComtorJDBCForeingFieldByJoin {

    private String attributeName;
    private String joinAlias;
    private String foreingFieldName;

    public ComtorJDBCForeingFieldByJoin(String attributeName, String joinAlias, String foreingFieldName) {
        this.attributeName = attributeName;
        this.joinAlias = joinAlias;
        this.foreingFieldName = foreingFieldName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
    }

    public String getForeingFieldName() {
        return foreingFieldName;
    }

    public void setForeingFieldName(String foreingFieldName) {
        this.foreingFieldName = foreingFieldName;
    }
    
    

}
