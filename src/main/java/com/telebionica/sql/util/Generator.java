/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.util;

import java.io.Serializable;

/**
 *
 * @author aldo
 */
public interface Generator {
    
    public Serializable next(String schema, String name);
    
}
