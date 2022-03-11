/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.query.AbstractQueryBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author aldo
 */
public class QueryBuilder extends AbstractQueryBuilder {

    private static final String ESQUEMA_RISTO = "risto";
    private static final String ESQUEMA_RST = "RSTX";

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(QueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        
        String username = "root";
        String password = "root";
        String database = ESQUEMA_RISTO;
        String hostname = "localhost";
        String port = "3306";
        
        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database;

        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    
}
