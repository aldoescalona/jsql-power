/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.order;


/**
 *
 * @author aldo
 */
public class Order {

    // private Query query;
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

    public String getOrderStatement(String fullColumnName) {
        return String.format("%s %s", fullColumnName, order);
    }

    public static Asc asc(String colname) {
        return new Asc(colname);
    }

    public static Desc desc(String colname) {
        return new Desc(colname);
    }
}
