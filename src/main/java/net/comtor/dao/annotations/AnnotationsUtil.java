package net.comtor.dao.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 *
 * @author dwin
 */
public class AnnotationsUtil {

    /**
     * Valida si a variable contiene los metodos get y set
     *
     * @param field
     * @return true si contiene los dos metodos.
     */
    public static boolean containsGetAndSetMethod(Field field, Class clazz) {
        String attributeName = field.getName();
        attributeName = capitalize(attributeName);
        Class[] setParameters = new Class[1];
        setParameters[0] = field.getType();

        Class[] getParameters = new Class[0];
        try {
            if ((clazz.getMethod("set" + attributeName, setParameters) != null)
                    && (clazz.getMethod("get" + attributeName, getParameters) != null)) {
                return true;
            }
        } catch (NoSuchMethodException e) {
        }

        try {
            /* 2014-05-05 jorgegarcia: Para que soporte los casos en que los atributos son char[] (por consumo de memoria) y los parametros
             * del metodo set es un String. */
            if (field.getType().equals(char[].class)
                    && (clazz.getMethod("set" + attributeName, String.class) != null)
                    && (clazz.getMethod("get" + attributeName, getParameters) != null)) {
                return true;
            }
        } catch (NoSuchMethodException e) {
        }

        try {
            if ((clazz.getMethod("set" + attributeName, setParameters) != null)
                    && (clazz.getMethod("is" + attributeName, getParameters) != null)) {
                return true;
            }
        } catch (NoSuchMethodException e) {
        }

        return false;
    }

    /**
     * Valida si la valiable es metodo is
     *
     * @param field
     * @return
     */
    public static boolean isMethodBoolean(Field field, Class clazz) {
        String attributeName = field.getName();
        attributeName = capitalize(attributeName);
        Class[] setParameters = new Class[1];
        setParameters[0] = field.getType();
        Class[] getParameters = new Class[0];
        try {
            if ((clazz.getMethod("set" + attributeName, setParameters) != null)
                    && (clazz.getMethod("is" + attributeName, getParameters) != null)) {
                return true;
            }
        } catch (NoSuchMethodException e) {
        }
        return false;
    }

    /**
     *
     * @param string
     * @return
     */
    public static String capitalize(String string) {
        if (string == null || string.trim().length() <= 0) {
            return "";
        }
        byte[] bytes = string.trim().getBytes();
        String newString = "";
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            char ch = (char) b;
            if (i == 0) {
                ch = Character.toUpperCase(ch);
            }
            newString += ch;
        }
        return newString;
    }

    /**
     *
     * @param entityBeanType
     * @return
     */
    public static String getKeyName(Class entityBeanType) {
        Field fields[] = entityBeanType.getDeclaredFields();
        for (Field field : fields) {
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(ComtorId.class)) {
                    return field.getName();
                }
            }
        }
        return null;
    }
}
