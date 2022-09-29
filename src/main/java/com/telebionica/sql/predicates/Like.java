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
import java.util.List;

/**
 *
 * @author aldo
 */
public class Like extends Predicate {

    private String fieldName;
    private PowerColumnType aliasColumnType;
    private Object value;
    private MATCH_MODE match_mode;

    private List<PowerColumnType> types;

    public Like(String fieldName, Object value, MATCH_MODE match_mode) {
        this.fieldName = fieldName;
        this.value = value;
        this.match_mode = match_mode;
    }

    @Override
    public String getPredicateStatement() {
        return String.format("%s LIKE CONCAT('%s',?,'%s')", aliasColumnType.getFullColumnName(), match_mode.getLeft(), match_mode.getRight());
    }

    @Override
    public boolean hasValues() {
        return true;
    }

    @Override
    public List<PowerColumnType> getValueTypes() {
        return types;
    }

    @Override
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {
        types = new ArrayList();
        aliasColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery().getEntityClass(), getQuery().getRootAlias(), rootJoinNodes, conn);
        if (aliasColumnType == null) {
            throw new PowerQueryException("No existe el atributo " + fieldName);
        }

        types.add(new PowerColumnType(aliasColumnType.getColumnType(), value));
    }
    
    public static enum MATCH_MODE {
        START("", "%"), END("%", ""), ANY("%", "%");

        private MATCH_MODE(String left, String right) {
            this.left = left;
            this.right = right;
        }

        public String getLeft() {
            return left;
        }

        public String getRight() {
            return right;
        }
        
        private String left;
        private String right;
    }
}
