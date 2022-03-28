/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.dialect;

import com.telebionica.sql.power.PowerManager;

/**
 *
 * @author aldo
 */
public abstract class Dialect {

    protected final PowerManager pm;

    public Dialect(PowerManager pm) {
        this.pm = pm;
    }
    
    public abstract String limit(String schema, String queryString, Integer first, Integer max);
    
    public String limit(String queryString, Integer first, Integer max){
        return limit(null, queryString, first, max);
    }

    public String limit(String queryString, Integer max) {
        return limit(queryString, null, max);
    }
    
    public abstract String nextVal(String schema, String seq);    
    
    public String nextVal(String seq){
        return nextVal(null, seq);
    }    
}
