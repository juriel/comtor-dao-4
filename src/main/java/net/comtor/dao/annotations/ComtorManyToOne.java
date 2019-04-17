/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

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
public @interface ComtorManyToOne {
    Cascade cascade() default Cascade.NONE;
    Class targetEntity();
    String joinColumn();
}
