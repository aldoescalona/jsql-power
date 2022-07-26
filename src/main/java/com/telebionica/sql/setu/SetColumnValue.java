/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.setu;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.PowerQueryException;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author aldo
 */
public class SetColumnValue extends SetForUpdate{
    
    private String fieldName;
    private String fieldValue;
    
    private PowerColumnType fieldColumnType;
    private PowerColumnType fieldValueColumnType;

    public SetColumnValue(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }
    
    @Override
    public String getAsignStatement() {
        return String.format("%s = %s", fieldColumnType.getFullColumnName(), fieldValueColumnType.getFullColumnName());
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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {
        fieldColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery().getEntityClass(), getQuery().getRootAlias(), rootJoinNodes, conn);
        if(fieldColumnType == null){
          throw new PowerQueryException("No existe el atributo " + fieldName);
        }
        
        fieldValueColumnType = getQuery().getPowerManager().getAliasColumnType(fieldValue, getQuery().getEntityClass(), getQuery().getRootAlias(), rootJoinNodes, conn);
        if(fieldValueColumnType == null){
          throw new PowerQueryException("No existe el atributo " + fieldValue);
        }
    }
}
