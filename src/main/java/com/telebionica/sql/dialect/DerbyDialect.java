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
public class DerbyDialect extends Dialect {

    public DerbyDialect(PowerManager pm) {
        super(pm);
    }

    @Override
    public String limit(String schema, String queryString, Integer first, Integer max) {

        if (first == null && max == null) {
            return queryString;
        }

        if (first == null && max != null) {
            return String.format("%s\nLIMIT %d", queryString, max);
        }

        if (first != null && max != null) {
            return String.format("%s\nLIMIT %d OFFSET %d", queryString, first, max);
        }
        return "";
    }

    @Override
    public String nextVal(String schema, String seq) {
        StringBuilder sb = new StringBuilder("VALUES NEXTVALUE FOR ");
        
        if(schema != null){
            sb.append(schema).append(".");
        }
        sb.append(seq);
        return sb.toString();
    }

}
