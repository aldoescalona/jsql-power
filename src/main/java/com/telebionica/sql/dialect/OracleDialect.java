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
public class OracleDialect extends Dialect {

    public OracleDialect(PowerManager pm) {
        super(pm);
    }

    @Override
    public String limit(String schema, String queryString, Integer first, Integer max) {

        if (first == null && max == null) {
            return queryString;
        }

        if (first == null && max != null) {
            return String.format("%s\nFETCH NEXT %d ROWS ONLY ", queryString, max);
        }

        if (first != null && max != null) {
            return String.format("%s\nOFFSET %d ROWS FETCH NEXT %d ROWS ONLY", queryString, first, max);
        }
        return "";
    }

    @Override
    public String nextVal(String schema, String seq) {
        StringBuilder sb = new StringBuilder("SELECT ");
        
        if(schema != null){
            sb.append(schema).append(".");
        }
        sb.append(seq).append(".NEXTVAL FROM DUAL");
        return sb.toString();
    }

}
