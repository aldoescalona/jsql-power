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
public class And extends Junction {

    public And(Predicate p1, Predicate p2, Predicate... pn) {
        super(JUNCTION_TYPE.AND, p1, p2, pn);
    }
}
