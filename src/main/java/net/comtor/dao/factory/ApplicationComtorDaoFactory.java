package net.comtor.dao.factory;

import net.comtor.dao.ComtorJDBCDao;

/**
 *
 * @author dwin
 */
public class ApplicationComtorDaoFactory {

    private static ApplicationComtorDaoFactory factory = null;

    /**
     * Constructor para {@link ApplicationComtorDaoFactory}.
     */
    private ApplicationComtorDaoFactory() {
    }

    /**
     * @return the factory
     */
    public static ApplicationComtorDaoFactory getFactory() {
        if (factory == null) {
            factory = new ApplicationComtorDaoFactory();
        }
        
        return factory;
    }

    public ComtorJDBCDao buildComtorDao(Class<? extends ComtorJDBCDao> clazz) {
        try {            
            return clazz.newInstance();
        } catch (InstantiationException ex) {            
            ex.printStackTrace();
        } catch (IllegalAccessException ex) {            
            ex.printStackTrace();
        }
        
        return null;
    }
}
