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
public class AliasColumnType {

    private String alias;
    private ColumnType columnType;

    public AliasColumnType(String alias, ColumnType columnType) {
        this.alias = alias;
        this.columnType = columnType;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public String getFullColumnName() {
        if (alias == null) {
            return columnType.getColumnName();
        } else {
            return String.format("%s.%s", alias, columnType.getColumnName());
        }
    }

}
