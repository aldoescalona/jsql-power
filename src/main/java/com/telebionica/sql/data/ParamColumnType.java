/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.data;

import com.telebionica.sql.type.ColumnType;

/**
 *
 * @author aldo
 */
public class ParamColumnType {
    
    private Object value;
    private String sql;
    private ColumnType columnType;


    public ParamColumnType(Object value, String sql, ColumnType columnType) {
        this.value = value;
        this.sql = sql;
        this.columnType = columnType;
    }
    
    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }
    
}
