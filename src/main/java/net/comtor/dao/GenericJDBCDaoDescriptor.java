package net.comtor.dao;

import java.lang.reflect.Method;
import java.util.LinkedList;

public class GenericJDBCDaoDescriptor extends ComtorJDBCDaoDescriptor {

    String sequenceQuery;

    public GenericJDBCDaoDescriptor(String tableName, Class clazz) {
        super(tableName, clazz);

        Method methods[] = clazz.getMethods();

        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            String mname = m.getName();
            if (m.getName().startsWith("get") && m.getParameterTypes().length == 0) {
                String attributeName = mname.substring(3);
                Class[] parameters = new Class[1];
                parameters[0] = m.getReturnType();
                try {
                    if (clazz.getMethod("set" + attributeName, parameters) != null) {
                        attributeName = Character.toLowerCase(attributeName.charAt(0)) + attributeName.substring(1);
                        addField(new ComtorJDBCField(clazz, attributeName, attributeName, false));
                    }
                } catch (SecurityException e) {
                    e.printStackTrace();

                } catch (NoSuchMethodException e) {
                }
            } else if (m.getName().startsWith("is") && m.getParameterTypes().length == 0) {
                String attributeName = mname.substring(2);
                Class[] parameters = new Class[1];
                parameters[0] = m.getReturnType();
                try {
                    if (clazz.getMethod("set" + attributeName, parameters) != null) {
                        attributeName = Character.toLowerCase(attributeName.charAt(0)) + attributeName.substring(1);
                        addField(new ComtorJDBCField(clazz, attributeName, attributeName, true));
                    }
                } catch (SecurityException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                }
            }
        }
    }

    public ComtorDaoKey getKey(Object obj) {
        ComtorDaoKey key = new ComtorDaoKey();
        LinkedList<ComtorJDBCField> f = this.getFindFields();
        for (ComtorJDBCField field : f) {
            key.put(field.getAttributeName(), field.getValue(obj));
        }
        return key;
    }

    @Override
    public String getSequenceQuery() {
        return sequenceQuery;
    }

    public void setSequenceQuery(String sql) {
        this.sequenceQuery = sql;
    }

    /**
     * Establece que un campo es Llave Foranea del Elemento.
     *
     * @param fieldName Nombre del Campo.
     */
    public void setForeignKeyValueField(String fieldName) {
        this.getField(fieldName).setFindable(false);
        this.getField(fieldName).setInsertable(false);
        this.getField(fieldName).setUpdatable(false);
    }

    /**
     * Establece que un campo es la Llave Primaria del Elemento.
     *
     * @param fieldName Nombre del Campo.
     */
    public void setPrimaryKey(String fieldName) {
        this.getField(fieldName).setFindable(true);
    }
}
