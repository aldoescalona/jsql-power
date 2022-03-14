/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.AliasColumnType;
import com.telebionica.sql.query.Predicate;
import com.telebionica.sql.data.ParamColumnType;
import com.telebionica.sql.query.QueryBuilderException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Comparison extends Predicate {

    private String fieldName;
    private AliasColumnType aliasColumnType;
    
    private COMPARISON_OPERATOR operator;
    private Object value;
    private List<ParamColumnType> types = new ArrayList<ParamColumnType>();

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
    public List<ParamColumnType> getValueTypes() {
        return types;
    }

    @Override
    public void build() throws QueryBuilderException{
        types.clear();
        aliasColumnType = getQuery().getAliasColumnType(fieldName);
        if(aliasColumnType == null){
          aliasColumnType = getQuery().getAliasColumnType(fieldName);  throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        types.add(new ParamColumnType(value, aliasColumnType.getColumnType()));
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
        
        private String operator;
    }
    
}
