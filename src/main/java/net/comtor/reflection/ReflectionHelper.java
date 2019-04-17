package net.comtor.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 *
 * @author COMTOR
 */
public class ReflectionHelper {

    private enum GetSet {

        GETTER,
        SETTER
    }

    public static Field[] getAllFields(Class clazz) {
        Field[] myFields = clazz.getDeclaredFields();
        Class superClazz = clazz.getSuperclass();
        
        if (superClazz != Object.class) {
            Field[] superFields = getAllFields(superClazz);
            Field[] allFields = new Field[myFields.length + superFields.length];
            
            System.arraycopy(myFields, 0, allFields, 0, myFields.length);
            System.arraycopy(superFields, 0, allFields, myFields.length, superFields.length);
            
            myFields = allFields;
        }
        
        return myFields;
    }

    public static Annotation[] getAllAnnotations(Class clazz) {
        Annotation[] myAnnotations = clazz.getAnnotations();
        Class superClazz = clazz.getSuperclass();
        
        if (superClazz != Object.class) {
            Annotation[] superAnnotations = getAllAnnotations(superClazz);
            Annotation[] allAnnotations = new Annotation[myAnnotations.length + superAnnotations.length];
            
            System.arraycopy(myAnnotations, 0, allAnnotations, 0, myAnnotations.length);
            System.arraycopy(superAnnotations, 0, allAnnotations, myAnnotations.length, superAnnotations.length);
            
            myAnnotations = allAnnotations;
        }
        
        return myAnnotations;
    }

    /**
     * Finds the getter method for the field.
     *
     * @param clazz Class.
     * @param fieldName Field name.
     * @return Method or {@code null} if the method doesn't exist in the class.
     */
    public static Method findGetter(Class<?> clazz, String fieldName) {
        try {
            return clazz.getMethod(buildGetterMethodName(fieldName));
        } catch (NoSuchMethodException ex) {
            return null;
        }
    }

    /**
     * Finds the setter method for the field.
     *
     * @param clazz Class.
     * @param fieldName Field name.
     * @param parameterTypes Array of parameter types of the method.
     * @return Method or {@code null} if the method doesn't exist in the class.
     */
    public static Method findSetter(Class<?> clazz, String fieldName, Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(buildSetterMethodName(fieldName), parameterTypes);
        } catch (NoSuchMethodException ex) {
            return null;
        }
    }

    /**
     * Builds the getter method name for the field.
     *
     * @param fieldName Field name.
     * @return Getter method name.
     */
    public static String buildGetterMethodName(String fieldName) {
        return buildGetterSetterName(GetSet.GETTER, fieldName);
    }

    /**
     * Builds the setter method name for the field.
     *
     * @param fieldName Field name.
     * @return Setter method name.
     */
    public static String buildSetterMethodName(String fieldName) {
        return buildGetterSetterName(GetSet.SETTER, fieldName);
    }

    /**
     * Builds the getter or setter method name for the field.
     *
     * @param getSet Getter or Setter flag.
     * @param fieldName Field name.
     * @return Getter/Setter method name.
     */
    private static String buildGetterSetterName(GetSet getSet, String fieldName) {
        return (getSet.equals(GetSet.GETTER) ? "g" : "s") + "et" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
    }
}
