/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author aldo
 */
public class TableType {
    
    private String name;
    protected Class entityClass;
    private List<ColumnType> columns;
    
    

    public TableType(String name, Class entityClass) {
        this.name = name;
        this.entityClass = entityClass;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public List<ColumnType> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnType> columns) {
        this.columns = columns;
    }
    
    public ColumnType getFieldColumnType(String fieldName){
        ColumnType ct = columns.stream().filter(e->e.getFieldName().equals(fieldName)).findAny().orElse(null);
        return ct;
    }
    
    public List<ColumnType> getIdColumns(){
        List<ColumnType> ids = columns.stream().filter(e -> e.isPrimary()).collect(Collectors.toList());
        return ids;
        
    }
    
}
