package com.telebionica.validator;

import java.util.List;

public class Messages {

    private String fieldName;
    private List<Message> messages;
    
    public Messages(String fieldName, List<Message> messages) {
        this.fieldName = fieldName;
        this.messages = messages;
    }

    public Messages() {
        super();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    

}
