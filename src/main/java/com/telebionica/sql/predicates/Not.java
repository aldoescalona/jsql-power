/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.PowerQueryException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Not extends Predicate {

    private Junction.JUNCTION_TYPE junctionType;
    private List<Predicate> predicates = new ArrayList();
    private String sql;
    private List<PowerColumnType> types = new ArrayList<PowerColumnType>();
    
    public Not(Junction.JUNCTION_TYPE junctionType, Predicate... pn) {
        this.junctionType = junctionType;
        predicates.addAll(Arrays.asList(pn));
    }

    public Not add(Predicate p) {
        predicates.add(p);
        return this;
    }

    @Override
    public String getPredicateStatement() {
        return sql;
    }

    @Override
    public boolean hasValues() {
        return !types.isEmpty();
    }

    @Override
    public List getValueTypes() {
        return types;
    }

    
    @Override
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {

        types.clear();
        
        StringBuilder sb = new StringBuilder("NOT(");

        Iterator<Predicate> it = predicates.iterator();

        while (it.hasNext()) {
            Predicate p = it.next();
            p.setQuery(getQuery());
            p.build(rootJoinNodes, conn);
            
            sb.append(p.getPredicateStatement());
            if (p.hasValues()) {
                types.addAll(p.getValueTypes());
            }
            if (it.hasNext()) {
                sb.append(" ").append(junctionType.toString()).append(" ");
            }
        }
        sb.append(")");

        sql = sb.toString();
    }

}
