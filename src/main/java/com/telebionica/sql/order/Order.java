/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.order;

import com.telebionica.sql.data.AliasColumnType;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;

/**
 *
 * @author aldo
 */
public class Order {
    
    private Query query;
    private AliasColumnType aliasColumnType;
    
    private String fieldName;
    private String order;
   

    public Order(String colname, String order) {
        this.fieldName = colname;
        this.order = order;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getOrder() {
        return order;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
    
    public void build() throws QueryBuilderException{
        aliasColumnType = getQuery().getAliasColumnType(fieldName);
        if(aliasColumnType == null){
          aliasColumnType = getQuery().getAliasColumnType(fieldName);  throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
        // types.add(new ParamColumnType(value, aliasColumnType.getFullColumnName(), aliasColumnType.getColumnType()));
    }
    
    public String getOrderStatement(){
        return String.format("%s %s", aliasColumnType.getFullColumnName(), order);
    }
    
    public static Asc asc(String colname){
        return new Asc(colname);
    }
    
    public static Desc desc(String colname){
        return new Desc(colname);
    }
}
