package net.comtor.dao.generics;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Set;
import net.comtor.dao.ComtorDaoException;

/**
 *
 * @author jorgegarcia@comtor.net
 * @since Dec 18, 2013
 * @version
 */
public class CachedComtorDaoElementLogicFacade<E, PK extends Serializable> extends ComtorDaoElementLogicFacade<E, PK> {

    private long expiration;
    private LinkedHashMap<PK, CachedValue> map;
   
    public CachedComtorDaoElementLogicFacade(long expiration) {
        this.expiration = expiration;
        map = new LinkedHashMap<PK, CachedValue>();
    }

    @Override
    public synchronized E find(PK id) throws ComtorDaoException {
        CachedValue value = map.get(id);
        if (value != null) {
            value.resetMillis();
            return value.getObject();
        }



        E object = super.find(id);
        map.put(id, new CachedValue(object));
        return object;
    }

    @Override
    public void create(E entity) throws ComtorDaoException {
        super.create(entity);
        PK pk = getPK(entity);
        map.put(pk, new CachedValue(entity));
    }

    @Override
    public void edit(E entity) throws ComtorDaoException {
        super.edit(entity);
        PK pk = getPK(entity);
        map.put(pk, new CachedValue(entity));
    }

    @Override
    public void remove(E entity) throws ComtorDaoException {
        super.remove(entity);
        PK pk = getPK(entity);
        map.remove(pk);
    }

    public synchronized void cleanCache() {
        long current = System.currentTimeMillis();
        LinkedList<PK> toRemove = new LinkedList<PK>();
        Set<PK> keys = map.keySet();
        for (PK key : keys) {
            if (map.get(key).getMillis() < current - expiration) {
                toRemove.add(key);
            }
        }

        for (PK pk : toRemove) {
            map.remove(pk);
        }
    }
    

    private class CachedValue {

        private E object;
        private long millis;

        public CachedValue(E object) {
            this.object = object;
            millis = System.currentTimeMillis();
        }

        public long getMillis() {
            return millis;
        }

        public E getObject() {
            return object;
        }

        void resetMillis() {
            millis = System.currentTimeMillis();
        }
    }
}
