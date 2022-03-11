/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

/**
 *
 * @author aldo
 */
public class NotEq extends Comparison {

    public NotEq(String colname, Object value) {
        super(colname, COMPARISON_OPERATOR.NOT_EQ, value);
    }

}
