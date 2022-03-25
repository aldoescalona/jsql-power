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
import java.util.List;

/**
 *
 * @author aldo
 */
public class SetRaw extends SetForUpdate{
    
    private String raw;

    public SetRaw(String raw) {
        this.raw = raw;
    }
    
    @Override
    public String getAsignStatement() {
        return raw;
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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException {
    }
}
