/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.join.Join;
import com.telebionica.sql.order.Order;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Fetch<F> {
    
    private String collectionField;
    private String alias;
    private List<Join> joins = new ArrayList();
    private List<Order> orders = new ArrayList();

    public Fetch(String collectionField, String alias) {
        this.collectionField = collectionField;
        this.alias = alias;
    }
    
    public String getCollectionField() {
        return collectionField;
    }

    public void setCollectionField(String collectionField) {
        this.collectionField = collectionField;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public void setJoins(List<Join> joins) {
        this.joins = joins;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public Fetch<F>  join(String fieldPath, String alias) throws QueryBuilderException, SQLException {
        return join(fieldPath, alias, Query.JOINTYPE.INNER);
    }

    public Fetch<F>  left(String field, String alias) throws QueryBuilderException, SQLException {
        return join(field, alias, Query.JOINTYPE.LEFT);
    }

    public Fetch<F>  right(String field, String alias) throws QueryBuilderException, SQLException {
        return join(field, alias, Query.JOINTYPE.RIGHT);
    }

    public Fetch<F>  join(String fieldPath, String alias, Query.JOINTYPE jointype) throws QueryBuilderException, SQLException {

        if (jointype == null) {
            throw new QueryBuilderException("Jointype no puede ser nulo");
        }

        joins.add(new Join(fieldPath, alias, jointype));

        return this;
    }
    
    
}
