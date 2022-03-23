/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.setu;

/**
 *
 * @author aldo
 */
public class SetsForUpdate {
    
    public static SetValue value(String colname, Object value) {
        return new SetValue(colname, value);
    }
    
    public static SetColumnValue column(String colname, String colvalue) {
        return new SetColumnValue(colname, colvalue);
    }
    
    public static SetNull Null(String colname) {
        return new SetNull(colname);
    }
    
    public static SetRaw raw(String raw) {
        return new SetRaw(raw);
    }
}
