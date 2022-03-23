/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.setu;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.QueryBuilderException;

/**
 *
 * @author aldo
 */
public class SetNull extends SetForUpdate{
    
    private String fieldName;
    private PowerColumnType aliasColumnType;

    public SetNull(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public String getAsignStatement() {
        return String.format("%s = null", aliasColumnType.getFullColumnName());
    }

    @Override
    public boolean hasValue() {
        return false;
    }

    @Override
    public PowerColumnType getValueType() {
        return null;
    }

    @Override
    public void build() throws QueryBuilderException {
        aliasColumnType = getQuery().getAliasColumnType(fieldName);
        if(aliasColumnType == null){
          throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        aliasColumnType.setValue(null);
    }
}
