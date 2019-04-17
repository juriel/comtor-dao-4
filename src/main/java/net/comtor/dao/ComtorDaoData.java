package net.comtor.dao;

import java.io.Serializable;
import java.util.LinkedList;

/**
 *
 * @author jorgegarcia
 */
public class ComtorDaoData implements Serializable {

    private LinkedList<String> headers;
    private LinkedList<Object> types;
    private LinkedList<LinkedList<Object>> rows;

    public ComtorDaoData() {
        this.headers = new LinkedList<String>();
        this.types = new LinkedList<Object>();
        this.rows = new LinkedList<LinkedList<Object>>();
    }

    public LinkedList<String> getHeaders() {
        return new LinkedList<String>(headers);
    }

    public void setHeaders(LinkedList<String> headers) {
        this.headers = headers;
    }

    public LinkedList<LinkedList<Object>> getRows() {
        return rows;
    }

    public LinkedList<Object> getTypes() {
        return types;
    }

    public void setTypes(LinkedList<Object> types) {
        this.types = types;
    }

    public synchronized Object[] headersToArray() {
        return headers.toArray();
    }

    public synchronized int headersSize() {
        return headers.size();
    }

    public synchronized String removeHeader(int index) {
        return headers.remove(index);
    }

    public boolean removeHeader(Object o) {
        return headers.remove(o);
    }

    public synchronized boolean headersIsEmpty() {
        return headers.isEmpty();
    }

    public synchronized String getHeader(int index) {
        return headers.get(index);
    }

    public synchronized boolean addHeader(String e) {
        return headers.add(e);
    }

    public synchronized Object[] rowsToArray() {
        return rows.toArray();
    }

    public synchronized int rowsSize() {
        return rows.size();
    }

    public synchronized LinkedList<Object> removeRow(int index) {
        return rows.remove(index);
    }

    public boolean removeRow(Object o) {
        return rows.remove(o);
    }

    public synchronized boolean rowsIsEmpty() {
        return rows.isEmpty();
    }

    public synchronized LinkedList<Object> getRow(int index) {
        return rows.get(index);
    }

    public synchronized boolean addRow(LinkedList<Object> e) {
        return rows.add(e);
    }

    public synchronized Object[] typesToArray() {
        return types.toArray();
    }

    public synchronized int typesSize() {
        return types.size();
    }

    public synchronized Object removeType(int index) {
        return types.remove(index);
    }

    public boolean removeType(Object o) {
        return types.remove(o);
    }

    public synchronized boolean typesIsEmpty() {
        return types.isEmpty();
    }

    public synchronized Object getType(int index) {
        return types.get(index);
    }

    public synchronized boolean addType(Object e) {
        return types.add(e);
    }
}
