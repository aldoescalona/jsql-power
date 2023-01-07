package com.telebionica.validator;

import com.telebionica.validator.ann.AnnotationUtil;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

public class FieldValidator {

    private Object objeto;
    private Field field;
    private Object valor;
    private Class<?> tipo;

    public FieldValidator(Object objeto, Field field) {
        super();
        this.objeto = objeto;
        this.field = field;
        this.tipo = field.getType();
    }

    public Object getObjeto() {
        return objeto;
    }

    public void setObjeto(Object objeto) {
        this.objeto = objeto;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Object getValor() throws Exception {
        if (valor == null) {
            Class<?> theclass = objeto.getClass();
            String fname = field.getName();
            String mname = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
            Method method = theclass.getDeclaredMethod(mname);
            valor = method.invoke(objeto);
        }
        return valor;
    }

    public void setValor(Object valor) {
        this.valor = valor;
    }

    public Class<?> getTipo() {
        return tipo;
    }

    public void setTipo(Class<?> tipo) {
        this.tipo = tipo;
    }
    

    public List<Message> validate(Class<?> grupo) throws Exception {
        List<Message> msgs = new ArrayList<Message>();

        if (AnnotationUtil.tieneNotNull(field, grupo)) {
            if (getValor() == null) {
                NotNull ann = field.getAnnotation(NotNull.class);
                String key = Validator.noramalizeKeyMessage(ann.message(), objeto.getClass().getSimpleName(), field.getName(), "NotNull");
                msgs.add(new Message(key));
            }
        }

        if (AnnotationUtil.tieneSize(field, grupo)) {
            Size ann = field.getAnnotation(Size.class);
            int min = ann.min();
            int max = ann.max();

            if (getValor() instanceof String) {
                String val = (String) valor;
                if (val.length() < min || val.length() > max) {
                    // msgs.add(new Message(ann.message(), valor, min, max));
                    String key = Validator.noramalizeKeyMessage(ann.message(), objeto.getClass().getSimpleName(), field.getName(), "Size");
                    msgs.add(new Message(key, valor, min, max));
                }
            }

        }

        if (AnnotationUtil.tienePatron(field, grupo)) {
            Pattern ann = field.getAnnotation(Pattern.class);

            if (getValor() instanceof String) {
                String val = (String) valor;

                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(ann.regexp());

                Matcher matcher = pattern.matcher(val);
                if (!matcher.matches()) {
                    String key = Validator.noramalizeKeyMessage(ann.message(), objeto.getClass().getSimpleName(), field.getName(), "Pattern");
                    msgs.add(new Message(key, valor));
                }
            }
        }

        return msgs;
    }
    
}
