package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author ericson
 * ForeingFieldFromTable
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ComtorForeingFieldFromJoin {
    String tableAlias() default "";
    String columnName() default "";    
    String alias()      default "";
}
