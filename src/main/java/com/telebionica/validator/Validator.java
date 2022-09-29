package com.telebionica.validator;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Validator {

    private static final Logger logger = Logger.getLogger(Validator.class.getName());

    

    private Object doubtObject;
    private Class group;

    private static final String VALIDATION_ERROR = "Validation.ERROR";
    private static final String EMPTY_VALUE = "SIN VALOR";

    private List<Messages> messagesList = new ArrayList();

    public Validator() {
    }

    public Validator validate(Object doubtObject, Class<?> group) {
        this.doubtObject = doubtObject;
        this.group = group;

        messagesList.clear();
        return this.morphometrics();
    }

    public Validator validate(Object doubtObject) {
        return validate(doubtObject, null);
    }

    private Validator morphometrics() {

        List<Message> rootMessages = new ArrayList<Message>();
        Class<?> theclass = doubtObject.getClass();

        Field[] scopeFields = theclass.getDeclaredFields();
        for (Field scopeField : scopeFields) {

            FieldValidator vf = new FieldValidator(doubtObject, scopeField);

            try {
                List<Message> msgs = vf.validate(group);
                if (!msgs.isEmpty()) {

                    Messages messages = new Messages(vf.getField().getName(), msgs);
                    messagesList.add(messages);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.toString(), e);
                try {
                    rootMessages.add(new Message(VALIDATION_ERROR, vf.getValor()));
                } catch (Exception e1) {
                    logger.log(Level.SEVERE, e.toString(), e);
                    rootMessages.add(new Message(VALIDATION_ERROR, EMPTY_VALUE));
                }
            }
        }

        if (!rootMessages.isEmpty()) {
            Messages messages = new Messages("root", rootMessages);
            messagesList.add(messages);
        }

        return this;
    }
    
    public List<Messages> translate(String locale){
        return Brocal.translate(messagesList, locale);
    }
    
}
