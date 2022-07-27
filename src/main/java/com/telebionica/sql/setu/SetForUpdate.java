/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.setu;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.PowerQueryException;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author aldo
 */
public abstract class SetForUpdate {
    
    private Query query;
    
    public abstract String getAsignStatement();
    public abstract boolean hasValue();
    public abstract PowerColumnType getValueType();
    public abstract void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException;

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
    
}
