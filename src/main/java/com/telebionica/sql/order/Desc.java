/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.order;

/**
 *
 * @author aldo
 */
public class Desc extends Order{

    public Desc(String colname) {
        super(colname, "DESC");
    }
    
}
