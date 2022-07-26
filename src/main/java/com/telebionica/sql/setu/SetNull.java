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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {
        aliasColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery().getEntityClass(), getQuery().getRootAlias(), rootJoinNodes, conn);
        if(aliasColumnType == null){
          throw new PowerQueryException("No existe el atributo " + fieldName);
        }
        aliasColumnType.setValue(null);
    }
}
