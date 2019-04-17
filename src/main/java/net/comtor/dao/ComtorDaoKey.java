package net.comtor.dao;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * This represents a search key
 *
 * @author juriel
 */
public class ComtorDaoKey implements Serializable {

    private HashMap<String, Object> keys;

    public ComtorDaoKey(String key, Object value) {
        init();
        put(key, value);
    }

    public ComtorDaoKey() {
        init();
    }

    /**
     * @param key
     * @param value
     */
    public void put(String key, Object value) {
        getKeys().put(key, value);
    }

    /**
     *
     */
    private void init() {
        setKeys(new HashMap<String, Object>());
    }

    /**
     * @param keys The keys to set.
     */
    void setKeys(HashMap<String, Object> keys) {
        this.keys = keys;
    }

    /**
     * @return Returns the keys.
     */
    public HashMap<String, Object> getKeys() {
        return keys;
    }

    public Object getValue(String key) {
        return keys.get(key);
    }
}
