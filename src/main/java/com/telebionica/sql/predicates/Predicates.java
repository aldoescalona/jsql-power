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
public class Predicates {

    public static Eq eq(String colname, Object value) {
        return new Eq(colname, value);
    }
    
     public static NotEq ne(String colname, Object value) {
        return new NotEq(colname, value);
    }
    
    public static Ge ge(String colname, Object value) {
        return new Ge(colname, value);
    }
    
    public static Gt gt(String colname, Object value) {
        return new Gt(colname, value);
    }
    
    public static Le le(String colname, Object value) {
        return new Le(colname, value);
    }
    
    public static Lt lt(String colname, Object value) {
        return new Lt(colname, value);
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
    
    public static And and(Predicate p1, Predicate p2, Predicate... pn) {
        return new And(p1, p2, pn);
    }
    
    public static Not not(Junction.JUNCTION_TYPE junction_type, Predicate... pn) {
        return new Not(junction_type, pn);
    }
    
    public static RawPredicate rawPredicate(String raw) {
        return new RawPredicate(raw);
    }
    
    public static EqEntity eqEntity(String colname, Object obj) {
        return new EqEntity(colname, obj);
    }
    
    public static Like like(String colname, Object value, Like.MATCH_MODE match_mode) {
        return new Like(colname, value, match_mode);
    }
}
