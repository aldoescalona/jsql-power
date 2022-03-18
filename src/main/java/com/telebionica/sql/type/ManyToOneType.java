/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.QueryBuilderException;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class ManyToOneType {
    
    private String fieldName;
    protected Class fieldClass; 
    protected List<JoinColumn> joiners;
    protected TableType tableType;

    public ManyToOneType(String fieldName, Class fieldClass, List<JoinColumn> joiners, TableType tableType) {
        this.fieldName = fieldName;
        this.fieldClass = fieldClass;
        this.joiners = joiners;
        this.tableType = tableType;
    }
    

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Class getFieldClass() {
        return fieldClass;
    }

    public void setFieldClass(Class fieldClass) {
        this.fieldClass = fieldClass;
    }

    public List<JoinColumn> getJoiners() {
        return joiners;
    }

    public void setJoiners(List<JoinColumn> joiners) {
        this.joiners = joiners;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }
    
    public Object getter(Object b) throws QueryBuilderException {
        Object obj = null;
        try {
            Method m = getReadMethod();
            obj = m.invoke(b);
        } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException | IntrospectionException ex) {
            Logger.getLogger(JoinNode.class.getName()).log(Level.SEVERE, null, ex);
            throw new QueryBuilderException(ex);
        }
        return obj;
    }
    
    public void setter(Object obj, Object child) throws QueryBuilderException {
        try {
            Method m = getWriteMethod();
            m.invoke(obj, child);
        } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException | IntrospectionException ex) {
            Logger.getLogger(JoinNode.class.getName()).log(Level.SEVERE, null, ex);
            throw new QueryBuilderException(ex);
        }
    }
    
    public Method getWriteMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(tableType.getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(fieldName)) {
                return pd.getWriteMethod();
            }
        }
        return null;
    }

    public Method getReadMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(tableType.getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(fieldName)) {
                return pd.getReadMethod();
            }
        }
        return null;
    }
}
