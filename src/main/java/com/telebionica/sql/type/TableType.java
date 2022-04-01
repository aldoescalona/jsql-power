/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import java.util.ArrayList;
import java.util.Arrays;
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
    private List<JoinColumnsType> joinColumns;
    private List<ManyToManyType> manyToManyTypes;
    

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
    
    public List<ColumnType> getFilterColumns(String ...fieldNames) {
        
        List<ColumnType> filterList;
        if (fieldNames == null || fieldNames.length == 0) {
                filterList = getColumns();
            } else {
                List<ColumnType> ids = getIdColumns();
                List<String> scls = Arrays.asList(fieldNames);
                List<ColumnType> cols = scls.stream().map(n -> getFieldColumnType(n)).collect(Collectors.toList());
                cols.removeAll(ids);
                filterList = new ArrayList();
                filterList.addAll(ids);
                filterList.addAll(cols);
            }
        
        return filterList;
    }

    public void setColumns(List<ColumnType> columns) {
        this.columns = columns;
    }

    public List<JoinColumnsType> getJoinColumns() {
        return joinColumns;
    }

    public void setJoinColumns(List<JoinColumnsType> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public List<ManyToManyType> getManyToManyTypes() {
        return manyToManyTypes;
    }

    public void setManyToManyTypes(List<ManyToManyType> manyToManyTypes) {
        this.manyToManyTypes = manyToManyTypes;
    }
    
    
    
    public ColumnType getFieldColumnType(String fieldName){
        ColumnType ct = columns.stream().filter(e->e.getFieldName().equals(fieldName)).findAny().orElse(null);
        return ct;
    }
    
    public ColumnType getColumnType(String columnName){
        ColumnType ct = columns.stream().filter(e->e.getColumnName().equals(columnName)).findAny().orElse(null);
        return ct;
    }
    
    public ManyToManyType getManyToManyType(String fieldName){
        ManyToManyType m2mt = manyToManyTypes.stream().filter(e-> e.getFieldName().equals(fieldName)).findAny().orElse(null);
        return m2mt;
    }
    
    public List<ColumnType> getIdColumns(){
        List<ColumnType> ids = columns.stream().filter(e -> e.isPrimary()).collect(Collectors.toList());
        return ids;
    }
    
    public JoinColumnsType getJoinColumnsType(String fieldName){
        JoinColumnsType ct = joinColumns.stream().filter(e->e.getFieldName().equals(fieldName)).findAny().orElse(null);
        return ct;
    }
}
