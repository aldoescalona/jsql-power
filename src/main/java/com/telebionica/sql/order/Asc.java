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
public class Asc extends Order{

    public Asc(String colname) {
        super(colname, "ASC");
    }
}
