package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author juriel
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ComtorForeingFieldByJoin {
    String joinAlias();
    /**
     * Si el join pertenece a una clase este es el nombre del campo en la clase 
     * foranea.
     * Si el join solo tiene tableName es el nombre de la columna
     * @return 
     */
    String foreingFieldName();    
}
