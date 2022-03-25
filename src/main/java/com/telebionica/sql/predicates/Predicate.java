/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 *
 * @author aldo
 */
public abstract class Predicate {
    
    private Query query;
    
    public abstract String getPredicateStatement();
    public abstract boolean hasValues();
    public abstract List<PowerColumnType> getValueTypes();
    public abstract void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException, SQLException;

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
    
}
