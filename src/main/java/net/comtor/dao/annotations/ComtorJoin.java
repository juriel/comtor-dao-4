package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import net.comtor.dao.ComtorJDBCJoin;

/**
 *
 * @author juriel
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ComtorJoin {
 
  
    String alias();
    String tableName() default "N/A";
    Class referencesClass() default Object.class;
    ComtorJDBCJoin.JOIN_TYPE joinType();
    String onClause();
}
