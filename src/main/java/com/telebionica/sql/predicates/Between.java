/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.QueryBuilderException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Between extends Predicate {

    private String fieldName;
    private PowerColumnType aliasColumnType;
    private Object value1;
    private Object value2;

    private List<PowerColumnType> types = new ArrayList<PowerColumnType>();

    public Between(String fieldName, Object value1, Object value2) {
        this.fieldName = fieldName;
        this.value1 = value1;
        this.value2 = value2;
    }

    @Override
    public String getPredicateStatement() {
        return String.format("%s between ? and ?", aliasColumnType.getFullColumnName());
    }

    @Override
    public boolean hasValues() {
        return true;
    }

    @Override
    public List<PowerColumnType> getValueTypes() {
        return types;
    }

    @Override
    public void build() throws QueryBuilderException {
        types.clear();
        aliasColumnType = getQuery().getAliasColumnType(fieldName);
        if (aliasColumnType == null) {
            aliasColumnType = getQuery().getAliasColumnType(fieldName);
            throw new QueryBuilderException("No existe el atributo " + fieldName);
        }

        types.add(new PowerColumnType(aliasColumnType.getColumnType(), value1));
        types.add(new PowerColumnType(aliasColumnType.getColumnType(), value2));
    }
}
