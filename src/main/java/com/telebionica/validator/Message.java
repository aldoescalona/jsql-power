package com.telebionica.validator;

public class Message {

    private String key;
    private String message;
    private Object value;
    private Object p1;
    private Object p2;

    public Message(String key) {
        this.key = key;
    }

    public Message(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public Message(String key, Object value, Object p1) {
        this.key = key;
        this.value = value;
        this.p1 = p1;
    }

    public Message(String key, Object value, Object p1, Object p2) {
        this.key = key;
        this.value = value;
        this.p1 = p1;
        this.p2 = p2;
    }
    
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getP1() {
        return p1;
    }

    public void setP1(Object p1) {
        this.p1 = p1;
    }

    public Object getP2() {
        return p2;
    }

    public void setP2(Object p2) {
        this.p2 = p2;
    }

    @Override
    public String toString() {
        return key;
    }

}
