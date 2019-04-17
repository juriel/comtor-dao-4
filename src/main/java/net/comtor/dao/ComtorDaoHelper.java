package net.comtor.dao;

import java.util.StringTokenizer;

public class ComtorDaoHelper {

    public static String getClassName(final Class clase) {
        StringTokenizer st = new StringTokenizer(clase.getName(), "[;");
        String name = "";
        while (st.hasMoreElements()) {
            name = st.nextToken();
        }
        return name;
    }
}
