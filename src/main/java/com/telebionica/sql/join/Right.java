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
public class Right extends Join{

    public Right(String fieldPath, String alias) {
        super(fieldPath, alias, Query.JOINTYPE.RIGHT);
    }
}
