/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import java.util.List;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class OneToManyType {
    
    private String fieldName;
    protected Class fieldClass; 
    protected Class collectionRelatedClass;
    protected List<JoinColumn> joiners;
    protected TableType tableType;
    

    public OneToManyType(String fieldName, Class fieldClass, Class collectionRelatedClass, List<JoinColumn> joiners, TableType tableType, boolean reverse) {
        this.fieldName = fieldName;
        this.fieldClass = fieldClass;
        this.collectionRelatedClass = collectionRelatedClass;
        this.joiners = joiners;
        this.tableType = tableType;
    }

    public OneToManyType(String fieldName, Class fieldClass, Class relatedClass, List<JoinColumn> joiners, TableType tableType) {
        this(fieldName, fieldClass, relatedClass, joiners, tableType, false);
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

    public Class getCollectionRelatedClass() {
        return collectionRelatedClass;
    }

    public void setCollectionRelatedClass(Class collectionRelatedClass) {
        this.collectionRelatedClass = collectionRelatedClass;
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

    
}
