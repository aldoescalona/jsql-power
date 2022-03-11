/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.query.Predicate;

/**
 *
 * @author aldo
 */
public class Or extends Junction {

    public Or(Predicate p1, Predicate p2, Predicate... pn) {
        super(JUNCTION_TYPE.OR, p1, p2, pn);
    }
}
