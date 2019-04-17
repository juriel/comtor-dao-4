package net.comtor.dao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import net.comtor.dao.ComtorJDBCDaoDescriptor;

/**
 *
 * @author dwin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ComtorSequence {

    public static final int POST_INSERT = ComtorJDBCDaoDescriptor.SEQUENCE_POST_INSERT;
    public static final int PRE_INSERT = ComtorJDBCDaoDescriptor.SEQUENCE_PRE_INSERT;

    String name();
    int typeInsert();
}
