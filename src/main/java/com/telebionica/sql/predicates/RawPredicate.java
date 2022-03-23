/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import java.util.List;

/**
 *
 * @author aldo
 */
public class RawPredicate extends Predicate {

    private String raw;

    public RawPredicate(String raw) {
        this.raw = raw;
    }

    @Override
    public String getPredicateStatement() {
        return raw;
    }

    @Override
    public boolean hasValues() {
        return false;
    }

    @Override
    public List getValueTypes() {
        return null;
    }

    @Override
    public void build() {
    }
}
