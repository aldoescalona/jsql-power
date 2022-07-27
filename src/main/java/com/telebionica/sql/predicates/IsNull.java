/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.PowerQueryException;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author aldo
 */
public class IsNull extends Predicate {

    private String colname;

    public IsNull(String colname) {
        this.colname = colname;
    }

    @Override
    public String getPredicateStatement() {
        return String.format("%s IS NULL", colname);
    }

    @Override
    public boolean hasValues() {
        return false;
    }

    @Override
    public List getValueTypes() {
        return null;
    }

    @Override
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException{
    }
}
