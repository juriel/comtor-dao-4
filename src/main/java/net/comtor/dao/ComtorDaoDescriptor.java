package net.comtor.dao;

/**
 *
 * Describes Object properties to ComtorDao
 *
 * @author root
 *
 */
public abstract class ComtorDaoDescriptor {

    private Class objectClass;

    public ComtorDaoDescriptor(Class clase) {
        this.objectClass = clase;
    }

    public ComtorDaoDescriptor() {
        objectClass = void.class;
    }

    public Class getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(Class objectClass) {
        this.objectClass = objectClass;
    }

    /**
     * Returns object key
     * @param obj
     * @return 
     */
    public abstract ComtorDaoKey getKey(Object obj);
}
