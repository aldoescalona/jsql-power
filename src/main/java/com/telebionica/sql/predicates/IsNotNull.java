/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.QueryBuilderException;
import java.sql.Connection;
import java.util.List;

/**
 *
 * @author aldo
 */
public class IsNotNull extends Predicate {

    private String colname;

    public IsNotNull(String colname) {
        this.colname = colname;
    }

    @Override
    public String getPredicateStatement() {
        return String.format("%s IS NOT NULL", colname);
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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException {
    }
}
