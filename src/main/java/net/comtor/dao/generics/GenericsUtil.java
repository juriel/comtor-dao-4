/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.comtor.dao.generics;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import net.comtor.dao.annotations.ComtorId;

/**
 *
 * @author dwin
 */
public class GenericsUtil {

    /**
     * 
     * @param element
     * @return
     */
    public static HashMap<String, Object> getKeyValues(Object element) {
        HashMap<String, Object> values = new LinkedHashMap<String, Object>();
        Field[] Fields = element.getClass().getDeclaredFields();
        Class[] arr = {};
        Object[] ceroParam = {};
        for (Field field : Fields) {
            try {
                ComtorId comtorId = field.getAnnotation(ComtorId.class);
                if (comtorId != null) {
                    String capName = Character.toUpperCase(field.getName().charAt(0)) + field.getName().substring(1);
                    Method method = element.getClass().getMethod("get" + capName, arr);
                    Object invoke = method.invoke(element, ceroParam);
                    if (invoke != null) {
                        values.put(field.getName(), invoke);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return values;
    }    
}
