/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.setu;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.QueryBuilderException;
import java.sql.Connection;
import java.sql.SQLException;
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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException, SQLException {
        fieldColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery(), rootJoinNodes, conn);
        if(fieldColumnType == null){
          throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        
        fieldValueColumnType = getQuery().getPowerManager().getAliasColumnType(fieldValue, getQuery(), rootJoinNodes, conn);
        if(fieldValueColumnType == null){
          throw new QueryBuilderException("No existe el atributo " + fieldValue);
        }
    }
}
