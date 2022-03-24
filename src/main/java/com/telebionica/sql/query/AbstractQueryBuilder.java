/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author aldo
 */
public abstract class AbstractQueryBuilder {


    public abstract Connection getConnection() throws SQLException;


    
}
