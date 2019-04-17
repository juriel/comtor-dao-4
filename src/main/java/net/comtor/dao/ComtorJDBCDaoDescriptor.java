package net.comtor.dao;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Describes relation between a Class and a Table
 *
 * @author COMTOR juriel and dwin (new bugs by dwin)
 *
 */
public abstract class ComtorJDBCDaoDescriptor extends ComtorDaoDescriptor {

    public static final int SEQUENCE_POST_INSERT = 1;
    public static final int SEQUENCE_PRE_INSERT = 2;
    public static final int SEQUENCE_NONE = 0;
    private String tableName;
    private LinkedList<ComtorJDBCField> fields;
    private HashMap<String, ComtorJDBCField> fieldsHash;
    private LinkedList<ComtorJDBCForeingField> foreingFields;
    private HashMap<String, ComtorJDBCForeingField> foreingFieldsHash;

    private LinkedList<ComtorJDBCJoin> joins;
    private LinkedHashMap<String, ComtorJDBCJoin> joinsMap;
    private LinkedHashMap<String, ComtorJDBCForeingFieldByJoin> foreingFieldsByJoinMap;
    private LinkedList< ComtorJDBCForeingFieldByJoin> foreingFieldsByJoin;

    private String sequenceQuery = null;
    private int sequenceTypeInsert = SEQUENCE_NONE;
    private String driver;

    /**
     *
     * @param tableName
     * @param clase
     */
    public ComtorJDBCDaoDescriptor(String tableName, Class clazz) {
        super(clazz);
        this.tableName = tableName;
        fields = new LinkedList<ComtorJDBCField>();
        fieldsHash = new HashMap<String, ComtorJDBCField>();
        foreingFields = new LinkedList<ComtorJDBCForeingField>();
        foreingFieldsHash = new HashMap<String, ComtorJDBCForeingField>();
        joins = new LinkedList<>();
        joinsMap = new LinkedHashMap<>();

        foreingFieldsByJoinMap = new LinkedHashMap<>();
        foreingFieldsByJoin = new LinkedList<>();
    }

    /**
     *
     */
    public ComtorJDBCDaoDescriptor() {
        super();
        fields = new LinkedList<ComtorJDBCField>();
        fieldsHash = new HashMap<String, ComtorJDBCField>();
        foreingFields = new LinkedList<ComtorJDBCForeingField>();
        foreingFieldsHash = new HashMap<String, ComtorJDBCForeingField>();
        joins = new LinkedList<>();
        joinsMap = new LinkedHashMap<>();
        foreingFieldsByJoinMap = new LinkedHashMap<>();
        foreingFieldsByJoin = new LinkedList<>();
    }

    /**
     *
     * @param column
     * @param obj
     * @return
     */
    public Object getFieldValue(String column, Object obj) {
        ComtorJDBCField field = fieldsHash.get(column);
        return field.getValue(obj);
    }

    /**
     *
     * @param index
     * @param obj
     * @return
     */
    public Object getFieldValue(int index, Object obj) {
        ComtorJDBCField field = fields.get(index);
        return field.getValue(obj);
    }

    /**
     * Adds a Field
     *
     * @param field
     */
    public void addField(ComtorJDBCField field) {
        fields.add(field);
        fieldsHash.put(field.getAttributeName(), field);
    }

    /**
     *
     * @param foreingField
     */
    public void addForeingField(ComtorJDBCForeingField foreingField) {
        foreingFields.add(foreingField);
        foreingFieldsHash.put(foreingField.getAttributeName(), foreingField);
    }

    /**
     *
     * @param key
     * @return
     */
    public ComtorJDBCField getField(String key) {
        return fieldsHash.get(key);
    }

    /**
     *
     * @param key
     * @return
     */
    public ComtorJDBCField getForeingField(String key) {
        return foreingFieldsHash.get(key);
    }

    /**
     * Removes a field using name
     *
     * @param name
     */
    public void removeField(String name) {
        fieldsHash.remove(name);
        LinkedList<ComtorJDBCField> toRemove = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.getAttributeName().equals(name)) {
                toRemove.add(field);
            }
        }
        fields.removeAll(toRemove);
    }

    /**
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     *
     * @param tableName
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     *
     * @return
     */
    LinkedList<ComtorJDBCField> getFields() {
        return fields;
    }

    /**
     *
     * @return
     */
    LinkedList<ComtorJDBCForeingField> getForeingFields() {
        return foreingFields;
    }

    /**
     * Returns vector of ComtorJDBCField insertables
     *
     * @return
     */
    public LinkedList<ComtorJDBCField> getInsertableFields() {
        LinkedList<ComtorJDBCField> ans = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.isInsertable()) {
                ans.add(field);
            }
        }
        return ans;
    }

    /**
     *
     * @return
     */
    public LinkedList<ComtorJDBCField> getFindFields() {
        LinkedList<ComtorJDBCField> ans = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.isFindable()) {
                ans.add(field);
            }
        }
        return ans;
    }

    /**
     *
     * @return
     */
    public LinkedList<ComtorJDBCField> getSelectableFields() {
        LinkedList<ComtorJDBCField> ans = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.isSelectable()) {
                ans.add(field);
            }
        }
        return ans;
    }

    // ComtorSpecialField 2014-02-10
    public LinkedList<ComtorJDBCField> getSpecialFields() {
        LinkedList<ComtorJDBCField> list = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.isSpecial()) {
                list.add(field);
            }
        }
        return list;
    }

    public void addJoin(ComtorJDBCJoin j) {
        joins.add(j);
        joinsMap.put(j.getAlias(), j);
    }

    public LinkedList<ComtorJDBCJoin> getJoins() {
        return joins;
    }

    public ComtorJDBCJoin getJoin(String joinAlias) {
        return joinsMap.get(joinAlias);
    }

    public void addForeingFieldByJoin(ComtorJDBCForeingFieldByJoin f) {
        foreingFieldsByJoinMap.put(f.getAttributeName(), f);
        foreingFieldsByJoin.add(f);
    }

    public LinkedList<ComtorJDBCForeingFieldByJoin> getForeingFieldsByJoin() {
        return foreingFieldsByJoin;
    }

    /**
     *
     * @return
     */
    public LinkedList<ComtorJDBCField> getUpdatebleFields() {
        LinkedList<ComtorJDBCField> ans = new LinkedList<ComtorJDBCField>();
        for (ComtorJDBCField field : fields) {
            if (field.isUpdatable()) {
                ans.add(field);
            }
        }
        return ans;
    }

    public String getSequenceQuery() {
        return sequenceQuery;
    }

    public int getSequenceTypeInsert() {
        return sequenceTypeInsert;
    }

    /**
     *
     * @param type SEQUENCE_POST_INSERT or SEQUENCE_PRE_INSERT
     * @param sequence sample: SELECT LAST_INSERT_ID() or SELECT
     * nextval('sq_user')
     */
    public void setSequence(int type, String sequence) {
        this.sequenceTypeInsert = type;
        this.sequenceQuery = sequence;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }
}
