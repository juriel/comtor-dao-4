
package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import net.comtor.dao.ComtorJDBCDao;

/**
 *
 * @author dwin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ComtorDaoFactory {
    Class<? extends ComtorJDBCDao> factory();
}
