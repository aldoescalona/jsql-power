/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.order;

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
public class Order {

    private Query query;
    private PowerColumnType aliasColumnType;

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

    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws QueryBuilderException, SQLException {
        aliasColumnType = query.getPowerManager().getAliasColumnType(fieldName, query, rootJoinNodes, conn);
        if (aliasColumnType == null) {
            throw new QueryBuilderException("No existe el atributo " + fieldName);
        }
    }

    public String getOrderStatement() {
        return String.format("%s %s", aliasColumnType.getFullColumnName(), order);
    }

    public static Asc asc(String colname) {
        return new Asc(colname);
    }

    public static Desc desc(String colname) {
        return new Desc(colname);
    }
}
