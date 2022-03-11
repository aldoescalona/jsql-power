/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.predicates.Between;
import com.telebionica.sql.predicates.Comparison;
import com.telebionica.sql.predicates.Eq;
import com.telebionica.sql.predicates.IsNotNull;
import com.telebionica.sql.predicates.IsNull;
import com.telebionica.sql.predicates.Or;
import com.telebionica.sql.predicates.RawPredicate;

/**
 *
 * @author aldo
 */
public class Predicates {

    public static Eq eq(String colname, Object value) {
        return new Eq(colname, value);
    }

    public static Comparison compare(String colname, Comparison.COMPARISON_OPERATOR operator, Object value) {
        return new Comparison(colname, operator, value);
    }

    public static IsNull isNUll(String colname) {
        return new IsNull(colname);
    }

    public static IsNotNull isNotNUll(String colname) {
        return new IsNotNull(colname);
    }

    public static Between between(String colname, Object obj1, Object obj2) {
        return new Between(colname, obj1, obj2);
    }
    
    public static Or or(Predicate p1, Predicate p2, Predicate... pn) {
        return new Or(p1, p2, pn);
    }
    
    public static RawPredicate rawPredicate(String raw) {
        return new RawPredicate(raw);
    }
}
