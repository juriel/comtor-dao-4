package net.comtor.dao.generics;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import net.comtor.dao.ComtorDaoException;
import net.comtor.dao.ComtorDaoKey;
import net.comtor.dao.ComtorJDBCDao;
import net.comtor.dao.ComtorJDBCDaoDescriptor;
import net.comtor.dao.annotations.AnnotationsJDBCDaoDescriptor;
import net.comtor.dao.annotations.AnnotationsUtil;
import net.comtor.dao.annotations.ComtorElement;
import net.comtor.dao.annotations.ComtorField;
import net.comtor.dao.annotations.ComtorId;
import net.comtor.dao.factory.ApplicationComtorDaoFactory;
import net.comtor.reflection.ReflectionHelper;

/**
 *
 * @author dwinpaez@comtor.net
 * @param <E>
 * @param <PK>
 */
public class ComtorDaoElementLogicFacade<E, PK extends Serializable> implements ComtorDaoLogicFacade<E, PK> {

    protected Class<E> elementType;
    protected Class<PK> pkType;
    protected AnnotationsJDBCDaoDescriptor descriptor;

    /**
     *
     */
    public ComtorDaoElementLogicFacade() {
        this(null);
    }

    public ComtorDaoElementLogicFacade(Class<? extends ComtorJDBCDao> classDao) {
        initTypes();
        initDescriptor(classDao);
        initDriver();
    }

    private void initDescriptor(Class<? extends ComtorJDBCDao> classDao) {
        try {
            if (classDao == null) {
                descriptor = new AnnotationsJDBCDaoDescriptor(elementType);
            } else {
                descriptor = new AnnotationsJDBCDaoDescriptor(elementType, classDao);
            }
        } catch (ComtorDaoException ex) {
            ex.printStackTrace();
        }
    }

    private void initDriver() {
        ComtorJDBCDao dao = null;
        try {
            dao = ApplicationComtorDaoFactory.getFactory().buildComtorDao(descriptor.getClassDao());
            descriptor.setDriver(dao.getDriver());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    private void initTypes() {
        this.elementType = (Class<E>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.pkType = (Class<PK>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    /**
     *
     * @return @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public ComtorJDBCDao getComtorJDBCDao() throws ComtorDaoException {
        Class<? extends ComtorJDBCDao> classDao = descriptor.getClassDao();

        return ApplicationComtorDaoFactory.getFactory().buildComtorDao(classDao);
    }

    /**
     *
     * @return
     */
    public Class<E> getElementType() {
        return elementType;
    }

    /**
     *
     * @return
     */
    public Class<PK> getPkType() {
        return pkType;
    }

    /**
     *
     * @param id
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public E find(PK id) throws ComtorDaoException {
        return findByProperty(AnnotationsUtil.getKeyName(elementType), id);
    }

    public E find(ComtorJDBCDao dao, PK id) throws ComtorDaoException {
        return findByProperty(dao, AnnotationsUtil.getKeyName(elementType), id);
    }

    public synchronized E findOrCreate(E object, ComtorDaoKey key) throws ComtorDaoException {
        E current = findByProperty(key);

        if (current != null) {
            return current;
        }

        create(object);

        return object;
    }

    /**
     * Finds an Entity by one of it's properties.
     *
     * @param property Property Name.
     * @param value Value
     * @return Entity.
     * @throws ComtorDaoException
     */
    @Override
    public E findByProperty(String property, Object value) throws ComtorDaoException {
        return findByProperty(new ComtorDaoKey(property, value));
    }

    public E findByProperty(ComtorJDBCDao dao, String property, Object value) throws ComtorDaoException {
        return findByProperty(dao, new ComtorDaoKey(property, value));
    }

    public E findByProperty(ComtorDaoKey daoKey) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            return findByProperty(dao, daoKey);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public E findByProperty(ComtorJDBCDao dao, ComtorDaoKey daoKey) throws ComtorDaoException {
        try {
            Object object = dao.findElement(daoKey, getTableDescriptorType());

            if (object == null) {
                return null;
            }

            if (elementType.isInstance(object)) {
                return (E) object;
            }

            throw new ComtorDaoException("Found Object (" + object.getClass().getName() + ") is not a " + elementType.getName() + " instance.");
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        }
    }

    @Override
    public LinkedList<E> findAllByProperty(String property, Object value) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            return findAllByProperty(dao, new ComtorDaoKey(property, value));
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    /**
     * Finds all objects by key.
     *
     * @param dao COMTOR JDBC DAO.
     * @param key COMTOR key.
     * @return List of object that have the specified key's values.
     * @throws ComtorDaoException
     * @since May 27, 2015
     */
    public LinkedList<E> findAllByProperty(ComtorJDBCDao dao, ComtorDaoKey key) throws ComtorDaoException {
        LinkedList<Object> list = dao.findAll(key, getTableDescriptorType());

        return toTypedList(list);
    }

    /**
     * Becomes the list of objects into a typed list.
     *
     * @param objects List of objects.
     * @return Typed list.
     * @since May 27, 2015
     */
    private LinkedList<E> toTypedList(LinkedList<Object> objects) {
        LinkedList<E> list = new LinkedList<>();

        for (Object object : objects) {
            list.add((E) object);
        }

        return list;
    }

    /**
     *
     * @param entity
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public void create(E entity) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            create(dao, entity);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public void create(Collection<E> entities) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            for (E entity : entities) {
                create(dao, entity);
            }
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public void create(ComtorJDBCDao dao, E entity) throws ComtorDaoException {
        dao.insertElement(entity, getTableDescriptorType());
    }

    /**
     *
     * @param entity
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public void edit(E entity) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();
            edit(dao, entity);
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public void edit(ComtorJDBCDao dao, E entity) throws ComtorDaoException {
        dao.updateElement(entity, getTableDescriptorType());
    }

    /**
     *
     * @param entity
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public void remove(E entity) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            remove(dao, entity);
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public void remove(ComtorJDBCDao dao, E entity) throws ComtorDaoException {
        dao.deleteElement(entity, getTableDescriptorType());
    }

    
    public LinkedList<E> findAll() throws ComtorDaoException {
        Object params [] = new Object[0];
        return findAll(getFindQuery(), params);
    }
    
    /**
     *
     * @param queryString
     * @param params
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public LinkedList<E> findAll(String queryString, Object... params) throws ComtorDaoException {
        return findAll(queryString, 0, -1, params);
    }

    public LinkedList<E> findAllSqllite(ComtorJDBCDao dao, String queryString, Object... params) throws ComtorDaoException {
        return findAllSqllite(dao, queryString, 0, -1, params);
    }

    public LinkedList<E> findAllSqllite(ComtorJDBCDao dao, String queryString, long firsResult, long maxResults, Object... params) throws ComtorDaoException {
        try {
            LinkedList<Object> objects = dao.findAllRangeSqlite(queryString, getTableDescriptorType(), firsResult, maxResults, params);

            LinkedList<E> entities = new LinkedList<>();

            for (Object object : objects) {
                if (elementType.isInstance(object)) {
                    entities.add((E) object);
                }
            }

            return entities;
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        }
    }

    /**
     *
     * @param queryString
     * @param firsResult
     * @param maxResults
     * @param params
     * @return
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public LinkedList<E> findAll(String queryString, long firsResult, long maxResults, Object... params) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            return findAll(dao, queryString, firsResult, maxResults, params);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    public LinkedList<E> findAll(ComtorJDBCDao dao, String queryString, long firsResult, long maxResults, Object... params) throws ComtorDaoException {
        try {
            LinkedList<Object> objects = dao.findAllRange(queryString, getTableDescriptorType(), firsResult, maxResults, params);

            LinkedList<E> entities = new LinkedList<>();

            for (Object object : objects) {
                if (elementType.isInstance(object)) {
                    entities.add((E) object);
                }
            }

            return entities;
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        }
    }

    /**
     *
     * @return @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public String getFindQuery() throws ComtorDaoException {
        return ComtorJDBCDao.getFindQuery(getTableDescriptorType());
    }

    /**
     *
     * @param queryString
     * @return @throws net.comtor.dao.ComtorDaoException
     */
    public long getCountElements(String queryString) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            return ComtorJDBCDao.countResultsQuery(dao, queryString);
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    /**
     *
     * @param queryString
     * @param params
     * @return @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public long getCountElements(String queryString, Object... params) throws ComtorDaoException {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();

            return ComtorJDBCDao.countResultsQuery(dao, queryString, params);
        } catch (Exception ex) {
            throw new ComtorDaoException(ex);
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    /**
     * Gets the Table Descriptor Type
     *
     * @return Table Descriptor Type's Class.
     * @throws net.comtor.dao.ComtorDaoException
     */
    @Override
    public ComtorJDBCDaoDescriptor getTableDescriptorType() throws ComtorDaoException {
        return descriptor;
    }

    /**
     *
     * @param entity
     * @param nameField
     */
//    public void loadChildElements(E entity, String nameField) {
//        Class clazz = getElementType();
//        ComtorJDBCDao dao = null;
//        try {
//            dao = getComtorJDBCDao();
//            Field field = clazz.getDeclaredField(nameField);
//            ComtorOneToMany annotation = field.getAnnotation(ComtorOneToMany.class);
//            if (annotation != null) {
//                Class fieldType = field.getType();
//                if (fieldType.equals(LinkedList.class)) {
//                    AnnotationsJDBCDaoDescriptor targetEntityDescriptor = new AnnotationsJDBCDaoDescriptor(annotation.targetEntity());
//                    targetEntityDescriptor.setDriver(descriptor.getDriver());
//                    Object values[] = GenericsUtil.getKeyValues(entity).values().toArray();
//                    if (values.length == annotation.joinColumn().length) {
//                        String wherePart = " WHERE 1 = 1";
//
//                        for (int i = 0; i < annotation.joinColumn().length; i++) {
//                            String joinColumn = annotation.joinColumn()[i];
//                            ComtorJDBCField comtorJDBCField = targetEntityDescriptor.getField(joinColumn);
//                            wherePart += " AND  " + targetEntityDescriptor.getTableName() + "." + comtorJDBCField.getColumnName() + " = " + getValue(values[i]);
//                        }
//
//                        LinkedList vector = (LinkedList) fieldType.newInstance();
//                        LinkedList<Object> childs = dao.findAllRange(ComtorJDBCDao.getFindQuery(targetEntityDescriptor) + wherePart, targetEntityDescriptor);
//                        vector.addAll(childs);
//
//                        Class[] arrSet = new Class[1];
//                        arrSet[0] = field.getType();
//                        String capName = Character.toUpperCase(field.getName().charAt(0)) + field.getName().substring(1);
//                        Method setMethod = entity.getClass().getMethod("set" + capName, arrSet);
//                        setMethod.invoke(entity, vector);
//                    }
//                }
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        } finally {
//            if (dao != null) {
//                dao.close();
//            }
//        }
//    }
    /**
     *
     * @param value
     * @return
     */
    protected String getValue(Object value) {
        Class type = value.getClass();

        if ((type.equals(long.class)) || (type.equals(Long.class)) || (type.equals(int.class)) || (type.equals(Integer.class))) {
            return String.valueOf(value);
        } else {
            return "'" + String.valueOf(value) + "'";
        }
    }

    /**
     * Ejecuta un query arbitrario, este puede ser usado por los hijos de esta
     * clase
     *
     * @param sql
     * @param params
     */
    protected void execute(String sql, Object... params) {
        ComtorJDBCDao dao = null;

        try {
            dao = getComtorJDBCDao();
            execute(dao, sql, params);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (dao != null) {
                dao.close();
            }
        }
    }

    protected void execute(ComtorJDBCDao dao, String sql, Object... params) throws ComtorDaoException {
        ComtorJDBCDao.execute(dao, sql, params);
    }

    public int getCount(PK key, Class<? extends Serializable> foreignClass, String fieldName) throws ComtorDaoException {
        ComtorJDBCDao dao = null;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            dao = getComtorJDBCDao();
            conn = dao.getJdbcConnection();
            String tableName = getTable(foreignClass);
            String column = getColumn(foreignClass, fieldName);
            String query = "\n"
                    + " SELECT \n"
                    + "     COUNT(1) \n"
                    + " FROM \n"
                    + "     " + tableName + " \n"
                    + " WHERE \n"
                    + "     " + tableName + "." + column + " = ? \n"
                    + "";

            ps = conn.prepareStatement(query);

            int pos = 1;
            ps.setObject(pos++, key);

            rs = ps.executeQuery();

            while (rs.next()) {
                return rs.getInt(1);
            }

            return 0;
        } catch (SQLException ex) {
            throw new ComtorDaoException(ex);
        } catch (NoSuchFieldException ex) {
            throw new ComtorDaoException(ex);
        } finally {
            ComtorJDBCDao.safeClose(dao, conn, ps, rs);
        }
    }

    private String getTable(Class<? extends Serializable> clazz) {
        ComtorElement element = clazz.getAnnotation(ComtorElement.class);

        return element.tableName();
    }

    private String getColumn(Class<? extends Serializable> clazz, String fieldName) throws NoSuchFieldException {
        Field field = clazz.getDeclaredField(fieldName);
        ComtorField comtorField = field.getAnnotation(ComtorField.class);

        if (comtorField == null) {
            return fieldName;
        }

        return comtorField.columnName();
    }

    public PK getPK(E object) throws ComtorDaoException {
        Field[] fields = ReflectionHelper.getAllFields(elementType);

        for (Field field : fields) {
            Annotation[] annotations = field.getAnnotations();

            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(ComtorId.class)) {
                    String keyName = field.getName();
                    String keyMethodName = "get" + keyName.substring(0, 1).toUpperCase() + keyName.substring(1, keyName.length());

                    try {
                        Method methodGet = elementType.getMethod(keyMethodName);

                        return (PK) methodGet.invoke(object);
                    } catch (Exception ex) {
                        throw new ComtorDaoException("Error getting Primary Key.", ex);
                    }
                }
            }
        }

        throw new ComtorDaoException(elementType.getName() + " does not have any ComtorId.");
    }
}
