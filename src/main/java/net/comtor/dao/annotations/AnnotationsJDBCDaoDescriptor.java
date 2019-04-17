package net.comtor.dao.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.LinkedList;
import net.comtor.dao.ComtorDaoException;
import net.comtor.dao.ComtorDaoKey;
import net.comtor.dao.ComtorJDBCDao;
import net.comtor.dao.ComtorJDBCDaoDescriptor;
import net.comtor.dao.ComtorJDBCField;
import net.comtor.dao.ComtorJDBCForeingField;
import net.comtor.dao.ComtorJDBCForeingFieldByJoin;
import net.comtor.dao.ComtorJDBCJoin;
import net.comtor.reflection.ReflectionHelper;

/**
 *
 * @author dwin
 */
public class AnnotationsJDBCDaoDescriptor extends ComtorJDBCDaoDescriptor {

    private Class clazz;
    private Class<? extends ComtorJDBCDao> classDao;

    public AnnotationsJDBCDaoDescriptor(Class clazz) throws ComtorDaoException {
        super();
        this.clazz = clazz;
        setObjectClass(clazz);
        readBasicAnnotations();
        readFieldsAnnotations();

    }

    public AnnotationsJDBCDaoDescriptor(Class clazz, Class<? extends ComtorJDBCDao> classDao) throws ComtorDaoException {
        super();
        this.clazz = clazz;
        setClassDao(classDao);
        setObjectClass(clazz);
        readBasicAnnotations();
        readFieldsAnnotations();
    }

    @Override
    public ComtorDaoKey getKey(Object obj) {
        ComtorDaoKey key = new ComtorDaoKey();
        LinkedList<ComtorJDBCField> f = this.getFindFields();
        for (ComtorJDBCField field : f) {
            key.put(field.getAttributeName(), field.getValue(obj));
        }
        return key;
    }

    private void addAttributesFromAnnotations(ComtorJDBCForeingField comtorForeingField, ComtorForeingField foreingField) {
        comtorForeingField.setReferencesClass(foreingField.referencesClass());
        comtorForeingField.setReferencesColumn(foreingField.referencesColumn());
        comtorForeingField.setForeingColumn(foreingField.foreingColumn());
        try {
            AnnotationsJDBCDaoDescriptor descriptor = new AnnotationsJDBCDaoDescriptor(comtorForeingField.getReferencesClass());
            comtorForeingField.setDescriptor(descriptor);
        } catch (ComtorDaoException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @throws net.comtor.dao.ComtorDaoException
     */
    private void readBasicAnnotations() throws ComtorDaoException {
        Annotation[] annotations = ReflectionHelper.getAllAnnotations(clazz); //clazz.getAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(ComtorElement.class)) {
                ComtorElement entity = (ComtorElement) annotation;
                super.setTableName(entity.tableName());
            }
            if (annotation.annotationType().equals(ComtorDaoFactory.class)) {
                ComtorDaoFactory factory = (ComtorDaoFactory) annotation;
                setClassDao(factory.factory());
            }
            if (annotation.annotationType().equals(ComtorJoin.class)) {
                ComtorJoin join = (ComtorJoin) annotation;
                ComtorJDBCJoin jDBCJoin = null;
                if (!join.referencesClass().equals(Object.class)) {
                    System.err.println("ComtorJDBCJoin jDBCJoin " + join.referencesClass());
                    jDBCJoin = new ComtorJDBCJoin(join.alias(), join.referencesClass(), join.joinType(), join.onClause());
                    AnnotationsJDBCDaoDescriptor descriptor = new AnnotationsJDBCDaoDescriptor(join.referencesClass());
                    jDBCJoin.setForeingClassDescriptor(descriptor);
                    System.err.println("ComtorJDBCJoin jDBCJoin descriptor " + descriptor.getTableName());

                } else {
                    jDBCJoin = new ComtorJDBCJoin(join.alias(), join.tableName(), join.joinType(), join.onClause());
                }
                addJoin(jDBCJoin);
            }
        }
        if (super.getTableName() == null) {
            throw new ComtorDaoException("Not ComtorEntity defined! -> " + clazz.getCanonicalName());
        }
        if (getClassDao() == null) {
            throw new ComtorDaoException("Not DaoFactory defined! -> " + clazz.getCanonicalName());
        }
    }

    /**
     *
     */
    private void readFieldsAnnotations() {
        Field fields[] = ReflectionHelper.getAllFields(clazz);//clazz.getDeclaredFields();
        for (Field field : fields) {
            if (AnnotationsUtil.containsGetAndSetMethod(field, clazz)) {
                String attributeName = field.getName();
                if (field.getAnnotation(ComtorForeingField.class) != null) {
                    ComtorForeingField foreingField = field.getAnnotation(ComtorForeingField.class);
                    ComtorJDBCForeingField comtorForeingField = new ComtorJDBCForeingField(clazz,
                            attributeName, attributeName, AnnotationsUtil.isMethodBoolean(field, clazz));
                    addAttributesFromAnnotations(comtorForeingField, foreingField);
                    addForeingField(comtorForeingField);

                    ComtorJDBCField comtorField = new ComtorJDBCField(clazz, attributeName, attributeName, AnnotationsUtil.isMethodBoolean(field, clazz));
                    comtorField.setFindable(false);
                    comtorField.setInsertable(false);
                    comtorField.setUpdatable(false);
                    addField(comtorField);
                } else if (field.getAnnotation(ComtorForeingFieldByJoin.class) != null) {
                    ComtorForeingFieldByJoin foreingFieldByJoin = (ComtorForeingFieldByJoin) field.getAnnotation(ComtorForeingFieldByJoin.class);
                    ComtorJDBCForeingFieldByJoin jDBCForeingField = new ComtorJDBCForeingFieldByJoin(attributeName, foreingFieldByJoin.joinAlias(), foreingFieldByJoin.foreingFieldName());
                    addForeingFieldByJoin(jDBCForeingField);
                    ComtorJDBCField comtorField = new ComtorJDBCField(clazz, attributeName, attributeName, AnnotationsUtil.isMethodBoolean(field, clazz));
                    comtorField.setFindable(false);
                    comtorField.setInsertable(false);
                    comtorField.setUpdatable(false);
                    addField(comtorField);

                } else {
                    ComtorJDBCField comtorField = new ComtorJDBCField(clazz, attributeName, attributeName, AnnotationsUtil.isMethodBoolean(field, clazz));
                    Annotation annotations[] = field.getAnnotations();
                    addAttributesFromAnnotations(annotations, comtorField);
                    addField(comtorField);
                }
            }
        }
    }

    /**
     *
     * @param annotations
     * @param comtorField
     */
    private void addAttributesFromAnnotations(Annotation[] annotations, ComtorJDBCField comtorField) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(ComtorField.class)) {
                ComtorField field = (ComtorField) annotation;
                if (field.columnName() != null && field.columnName().trim().length() > 0) {
                    comtorField.setColumnName(field.columnName());
                }
                comtorField.setInsertable(field.insertable());
                comtorField.setSelectable(field.selectable());
                comtorField.setUpdatable(field.updatable());
                comtorField.setNullable(field.nullable());
                comtorField.setNullValue(field.nullValue());
            } else if (annotation.annotationType().equals(ComtorOneToMany.class)) {
                comtorField.setFindable(false);
                comtorField.setInsertable(false);
                comtorField.setUpdatable(false);
                comtorField.setSelectable(false);
            } else if (annotation.annotationType().equals(ComtorSequence.class)) {
                ComtorSequence sequenceGenerator = (ComtorSequence) annotation;
                this.setSequence(sequenceGenerator.typeInsert(), sequenceGenerator.name());

                if (sequenceGenerator.typeInsert() == SEQUENCE_POST_INSERT) {
                    comtorField.setInsertable(false);
                } else if (sequenceGenerator.typeInsert() == SEQUENCE_PRE_INSERT) {
                    comtorField.setInsertable(true);
                }
            } else if (annotation.annotationType().equals(ComtorId.class)) {
                comtorField.setFindable(true);
            } // ComtorSpecialField 2014-02-10
            else if (annotation.annotationType().equals(ComtorSpecialField.class)) {
                comtorField.setFindable(false);
                comtorField.setInsertable(false);
                comtorField.setUpdatable(false);
                comtorField.setSelectable(false);
                comtorField.setSpecial(true);
            }
        }
    }

    public Class<? extends ComtorJDBCDao> getClassDao() {
        return classDao;
    }

    public void setClassDao(Class<? extends ComtorJDBCDao> classDao) {
        this.classDao = classDao;

    }
}
