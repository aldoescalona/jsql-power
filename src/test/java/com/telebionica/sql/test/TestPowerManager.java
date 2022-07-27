/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.dialect.MySQLDialect;
import com.telebionica.sql.power.PowerManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.telebionica.sql.power.Power;
import com.telebionica.sql.query.PowerQueryException;

/**
 *
 * @author aldo
 */
@Power(dialect = MySQLDialect.class)
public class TestPowerManager extends PowerManager {

    private static final String ESQUEMA_RISTO = "risto";
    private static final String ESQUEMA_RST = "RSTX";

    public TestPowerManager() {
        this.setDebugSQL(true);
    }

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(TestPowerManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Connection getConnection() throws PowerQueryException {
        
        String username = "root";
        String password = "root";
        String database = ESQUEMA_RISTO;
        String hostname = "localhost";
        String port = "3306";
        
        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database;

        Connection conn;
        try {
            conn = DriverManager.getConnection(url, username, password);
            return conn;
        }catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

}
