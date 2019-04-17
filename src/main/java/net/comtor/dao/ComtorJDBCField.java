package net.comtor.dao;

import java.beans.BeanDescriptor;
import java.lang.reflect.Method;

public class ComtorJDBCField {
    /*
     * B byte C char D double F float I int J long Lclassname; class or
     * interface S short Z boolean V void
     */

    public static final int TYPE_BYTE = 0;
    public static final int TYPE_CHAR = 1;
    public static final int TYPE_DOUBLE = 2;
    public static final int TYPE_FLOAT = 3;
    public static final int TYPE_INT = 4;
    public static final int TYPE_LONG = 5;
    public static final int TYPE_BOOLEAN = 6;
    public static final int TYPE_SHORT = 7;
    public static final int TYPE_VOID = 8;
    public static final int TYPE_STRING = 9;
    public static final int TYPE_BIGDECIMAL = 10;
    public static final int TYPE_DATE = 11;
    public static final int TYPE_TIME = 12;
    public static final int TYPE_TIMESTAMP = 13;
    public static final int TYPE_URL = 14;
    public static final int TYPE_CLASS = 15;
    // private String name;
    private String columnName;
    private String attributeName;
    private Class attributeType;
    private boolean insertable = true;
    private boolean updatable = true;
    private boolean selectable = true;
    private boolean findable = false;
    private boolean special = false; // ComtorSpecialField 2014-02-10
    static private Object[] ceroParam = {};
    private int intType;

    /**
     * This field must be null where numeric value is nullValue
     */
    private boolean nullable = false;
    private long nullValue = 0;

    /**
     * Holds value of property setMethod.
     */
    private Method setMethod;
    /**
     * Holds value of property getMethod.
     */
    private Method getMethod;

    /**
     * Creates a new instance of DBField
     *
     *
     * @param name Name of field in class
     * @param fieldName field name in table
     * @param tableName table name
     * @param type Class Type of Object
     * @getMethod Method to get
     * @setMethod Method to Set
     *
     */
    public ComtorJDBCField(String attributeName, String columnName, Class type, Method getMethod, Method setMethod) {
        // this.name = name;
        this.columnName = columnName;
        this.attributeName = attributeName;

        this.attributeType = type;
        this.intType = getTypeFromString(type.getName());
        if (getMethod == null) {
            this.updatable = false;
            this.selectable = false;
        }
        this.getMethod = getMethod;

        if (setMethod == null) {
            this.insertable = false;
            this.findable = false;
        }
        this.setMethod = setMethod;
    }

    /*
     * public ComtorJDBCField(Class clazz, String fieldName, String tableName) {
     * init(clazz, fieldName, tableName);
     *  }
     */
    private void init(Class clazz, String attributeName, String columnName) {
        // this.name = name;
        this.columnName = columnName;
        this.attributeName = attributeName;
        String capName = Character.toUpperCase(attributeName.charAt(0)) + attributeName.substring(1);
        Class[] arr = {};
        Class[] arrSet = new Class[1];
        this.attributeType = String.class;
        BeanDescriptor descriptor = new BeanDescriptor(clazz);
        try {
            this.attributeType = descriptor.getBeanClass().getMethod("get" + capName, arr).getReturnType();
            this.intType = getTypeFromString(attributeType.getName());
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.getMethod = null;
        this.setMethod = null;

        try {
            getMethod = descriptor.getBeanClass().getMethod("get" + capName, arr);
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        arrSet[0] = this.attributeType;
        try {
            setMethod = descriptor.getBeanClass().getMethod("set" + capName, arrSet);
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public ComtorJDBCField(Class clazz, String attributeName, String columnName, boolean isBoolean) {
        if (!isBoolean) {
            init(clazz, attributeName, columnName);
            return;
        }
        // this.name = name;
        this.columnName = columnName;
        this.attributeName = attributeName;
        String capName = Character.toUpperCase(attributeName.charAt(0)) + attributeName.substring(1);
        Class[] arr = {};
        Class[] arrSet = new Class[1];
        this.attributeType = String.class;
        BeanDescriptor descriptor = new BeanDescriptor(clazz);
        try {
            this.attributeType = descriptor.getBeanClass().getMethod("is" + capName, arr).getReturnType();
            this.intType = getTypeFromString(attributeType.getName());
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.getMethod = null;
        this.setMethod = null;

        try {
            getMethod = descriptor.getBeanClass().getMethod("is" + capName, arr);
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {

            e.printStackTrace();
        }

        arrSet[0] = this.attributeType;
        try {
            setMethod = descriptor.getBeanClass().getMethod("set" + capName, arrSet);
        } catch (SecurityException e) {

            e.printStackTrace();
        } catch (NoSuchMethodException e) {

            e.printStackTrace();
        }
    }

    public int getIntType() {
        return intType;
    }

    static public int getTypeFromString(String type) {
        if (type.equals("B") || type.equals("byte")) {
            return TYPE_BYTE;
        } else if (type.equals("C") || type.equals("char")) {
            return TYPE_CHAR;
        } else if (type.equals("D") || type.equals("double")) {
            return TYPE_DOUBLE;
        } else if (type.equals("F") || type.equals("float")) {
            return TYPE_FLOAT;
        } else if (type.equals("I") || type.equals("int")) {
            return TYPE_INT;
        } else if (type.equals("L") || type.equals("long")) {
            return TYPE_LONG;
        } else if (type.equals("S") || type.equals("short")) {
            return TYPE_SHORT;
        } else if (type.equals("Z") || type.equals("boolean")) {
            return TYPE_BOOLEAN;
        } else if (type.equals("V") || type.equals("void")) {
            return TYPE_VOID;
        } else if (type.equals("java.lang.String")) {
            return TYPE_STRING;
        } else if (type.equals("java.math.BigDecimal")) {
            return TYPE_BIGDECIMAL;
        } else if (type.equals("java.sql.Date")) {
            return TYPE_DATE;
        } else if (type.equals("java.sql.Time")) {
            return TYPE_TIME;
        } else if (type.equals("java.sql.Timestamp")) {
            return TYPE_TIMESTAMP;
        } else if (type.equals("java.net.URL")) {
            return TYPE_URL;
        } else {
            return TYPE_CLASS;
        }
    }

    /**
     * Getter for property setMethod.
     *
     * @return Value of property setMethod.
     *
     */
    public Method getSetMethod() {
        return this.setMethod;
    }

    /**
     * Setter for property setMethod.
     *
     * @param setMethod New value of property setMethod.
     *
     */
    public void setSetMethod(Method setMethod) {
        this.setMethod = setMethod;
    }

    /**
     * Getter for property getMethod.
     *
     * @return Value of property getMethod.
     *
     */
    public Method getGetMethod() {
        return this.getMethod;
    }

    /**
     * Setter for property getMethod.
     *
     * @param getMethod New value of property getMethod.
     *
     */
    public void setGetMethod(Method getMethod) {
        this.getMethod = getMethod;
    }

    /**
     * Getter for property classType.
     *
     * @return Value of property classType.
     *
     */
    public Class getType() {
        return attributeType;
    }

    /**
     * Setter for property classType.
     *
     * @param classType New value of property classType.
     */
    public void setType(Class classType) {
        this.attributeType = classType;
    }

    /**
     * @return
     */
    public String getAttributeName() {
        return attributeName;
    }

    /**
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @param string
     */
    public void setAttributeName(String string) {
        attributeName = string;
    }

    /**
     * @param string
     */
    public void setColumnName(String string) {
        columnName = string;
    }

    /**
     * @return
     */
    public boolean isFindable() {
        return findable;
    }

    /**
     * @return
     */
    public boolean isInsertable() {
        return insertable;
    }

    /**
     * @return
     */
    public boolean isSelectable() {
        return selectable;
    }

    /**
     * @return
     */
    public boolean isUpdatable() {
        return updatable;
    }

    /**
     * @param b
     */
    public void setFindable(boolean b) {
        findable = b;
    }

    /**
     * @param b
     */
    public void setInsertable(boolean b) {
        insertable = b;
    }

    /**
     * @param b
     */
    public void setSelectable(boolean b) {
        selectable = b;
    }

    /**
     * @param b
     */
    public void setUpdatable(boolean b) {
        updatable = b;
    }

    // ComtorSpecialField 2014-02-10
    public boolean isSpecial() {
        return special;
    }

    // ComtorSpecialField 2014-02-10
    public void setSpecial(boolean special) {
        this.special = special;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public long getNullValue() {
        return nullValue;
    }

    public void setNullValue(long nullValue) {
        this.nullValue = nullValue;
    }
  
    /**
     *
     * @param obj
     * @return
     */
    public Object getValue(Object obj) {
        try {
            Object resp = getGetMethod().invoke(obj, ceroParam);
            if (nullable  && attributeType.equals(long.class)){
                long value = ((Long)resp).longValue();
                if (value == nullValue){
                    return null;
                }
            }
             if (nullable  && attributeType.equals(int.class)){
                int value = ((Integer)resp).intValue();
                if (value == nullValue){
                    return null;
                }
            }
            return resp;
        } catch (Exception e) {
            System.out.println("Atributo " + this.attributeName + " Columna " + this.columnName + " " + this.getMethod.getName() + " " + obj);
            e.printStackTrace();
        }
        return null;
    }
}
