/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import javax.persistence.Enumerated;


/**
 *
 * @author aldo
 */
public class ColumnType {
    
    private String columnName;
    private String fieldName;
    protected Class fieldClass; 
    private Integer type;
    private Integer scale;
    private boolean primary;
    private GeneratorType generatorType;
    protected Enumerated enumerated;
    
    private boolean embedded;
    private String embeddedFieldName;
    protected Class embeddedFieldClass; 
    
    protected TableType tableType;

    public ColumnType(String columnName, String fieldName, Class clazz, TableType tableType) {
        this.columnName = columnName;
        this.fieldName = fieldName;
        this.fieldClass = clazz;
        this.tableType = tableType;
    }
    
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public Class getFieldClass() {
        return fieldClass;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    public GeneratorType getGeneratorType() {
        return generatorType;
    }

    public void setGeneratorType(GeneratorType generatorType) {
        this.generatorType = generatorType;
    }

    public Enumerated getEnumerated() {
        return enumerated;
    }

    public void setEnumerated(Enumerated enumerated) {
        this.enumerated = enumerated;
    }
    
    public boolean hasEnumerated(){
        return enumerated != null;
    }

    public boolean isEmbedded() {
        return embedded;
    }

    public void setEmbedded(boolean embedded) {
        this.embedded = embedded;
    }

    public String getEmbeddedFieldName() {
        return embeddedFieldName;
    }

    public void setEmbeddedFieldName(String embeddedFieldName) {
        this.embeddedFieldName = embeddedFieldName;
    }

    public Class getEmbeddedFieldClass() {
        return embeddedFieldClass;
    }

    public void setEmbeddedFieldClass(Class embeddedFieldClass) {
        this.embeddedFieldClass = embeddedFieldClass;
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
    
    public Method getEmbeddedWriteMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(tableType.getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(embeddedFieldName)) {
                return pd.getWriteMethod();
            }
        }
        return null;
    }
    
    public Method getEmbeddedIdWriteMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(embeddedFieldClass);
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
    
    public Method getEmbeddedReadMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(tableType.getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(embeddedFieldName)) {
                return pd.getReadMethod();
            }
        }
        return null;
    }
    
    public Method getEmbeddedIdReadMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(embeddedFieldClass);
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(fieldName)) {
                return pd.getReadMethod();
            }
        }
        return null;
    }
}
