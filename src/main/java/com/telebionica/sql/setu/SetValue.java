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
public class SetValue extends SetForUpdate{
    
    private String fieldName;
    private PowerColumnType aliasColumnType;
    private Object value;

    public SetValue(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }
    
    @Override
    public String getAsignStatement() {
        return String.format("%s = ?", aliasColumnType.getFullColumnName());
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @Override
    public PowerColumnType getValueType() {
        return aliasColumnType;
    }

    @Override
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException, SQLException {
        aliasColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery(), rootJoinNodes, conn);
        if(aliasColumnType == null){
          throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        aliasColumnType.setValue(value);
    }
}
