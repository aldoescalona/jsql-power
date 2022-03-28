/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.QueryBuilderException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Comparison extends Predicate {

    private String fieldName;
    private PowerColumnType aliasColumnType;
    
    private COMPARISON_OPERATOR operator;
    private Object value;
    private List<PowerColumnType> types;

    public Comparison(String fieldName, COMPARISON_OPERATOR operator, Object value) {
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String getPredicateStatement() {
        return String.format("%s %s ?", aliasColumnType.getFullColumnName(), operator.getOperator());
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
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException, SQLException{
        types = new ArrayList();
        aliasColumnType = getQuery().getPowerManager().getAliasColumnType(fieldName, getQuery(), rootJoinNodes, conn);
        if(aliasColumnType == null){
          throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        aliasColumnType.setValue(value);
        types.add(aliasColumnType);
    }
    
    public static enum COMPARISON_OPERATOR{
        EQ("="), 
        EQ_SAFENULL("<=>"), 
        NOT_EQ("<>"), 
        NOT_EQUAL("!="),
        GT(">"),
        GE(">="),
        LT("<"),
        LE("<=");

        private COMPARISON_OPERATOR(String operator) {
            this.operator = operator;
        }

        public String getOperator() {
            return operator;
        }
        
        private final String operator;
    }

    public Object getValue() {
        return value;
    }
    
}
