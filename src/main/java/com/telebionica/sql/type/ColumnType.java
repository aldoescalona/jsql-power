/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;


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

    public ColumnType(String columnName, String fieldName, Class clazz) {
        this.columnName = columnName;
        this.fieldName = fieldName;
        this.fieldClass = clazz;
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

    
    
}
