/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.join;

import com.telebionica.sql.query.Query;

/**
 *
 * @author aldo
 */
public class Join {
    
    private String fieldPath;
    private String alias;
    private Query.JOINTYPE jointype;

    public Join(String fieldPath, String alias, Query.JOINTYPE jointype) {
        this.fieldPath = fieldPath;
        this.alias = alias;
        this.jointype = jointype;
    }


    public String getFieldPath() {
        return fieldPath;
    }

    public void setFieldPath(String fieldPath) {
        this.fieldPath = fieldPath;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Query.JOINTYPE getJointype() {
        return jointype;
    }

    public void setJointype(Query.JOINTYPE jointype) {
        this.jointype = jointype;
    }
    
}
