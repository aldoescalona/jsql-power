/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import javax.persistence.JoinTable;

/**
 *
 * @author aldo
 */
public class ManyToManyType {
    
    private String fieldName;
    protected Class fieldClass; 
    protected Class collectionRelatedClass;
    protected JoinTable joinTable;
    protected TableType tableType;
    private boolean reverse;

    public ManyToManyType(String fieldName, Class fieldClass, Class relatedClass, JoinTable joinTable, TableType tableType, boolean reverse) {
        this.fieldName = fieldName;
        this.fieldClass = fieldClass;
        this.collectionRelatedClass = relatedClass;
        this.joinTable = joinTable;
        this.tableType = tableType;
        this.reverse = reverse;
    }

    public ManyToManyType(String fieldName, Class fieldClass, Class relatedClass, JoinTable joinTable, TableType tableType) {
        this(fieldName, fieldClass, relatedClass, joinTable, tableType, false);
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

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }
    
    
    /*public Object getter(Object b) throws QueryBuilderException {
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
    }*/

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }
}
