package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author dwin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ComtorForeingField {

    /**
     * class of the foreing table
     *
     * @return
     */

    Class referencesClass();

    /**
     * Column name of foreing value
     *
     * @return
     */
    String foreingColumn();

    /**
     * Local column to join with foreing table
     *
     * @return
     */
    String[] referencesColumn();

}
