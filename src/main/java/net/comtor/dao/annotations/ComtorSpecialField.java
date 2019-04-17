package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author jorgegarcia@comtor.net
 * @since Feb 10, 2014
 * @version
 */
// ComtorSpecialField 2014-02-10
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ComtorSpecialField {
}
