/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.util;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Aldo
 */
public class JDBCUtil {

    public static Integer getInteger(ResultSet rs, String strColName) throws SQLException {
        int nValue = rs.getInt(strColName);
        return rs.wasNull() ? null : nValue;
    }
    
    public static Long getLong(ResultSet rs, String strColName) throws SQLException {
        long nValue = rs.getLong(strColName);
        return rs.wasNull() ? null : nValue;
    }
    
    public static Float getFloat(ResultSet rs, String strColName) throws SQLException {
        float nValue = rs.getFloat(strColName);
        return rs.wasNull() ? null : nValue;
    }
    
    public static Double getDouble(ResultSet rs, String strColName) throws SQLException {
        double nValue = rs.getFloat(strColName);
        return rs.wasNull() ? null : nValue;
    }

    public static Boolean getBoolean(ResultSet rs, String strColName) throws SQLException {
        boolean nValue = rs.getBoolean(strColName);
        return rs.wasNull() ? null : nValue;
    }
    
    public static Byte getByte(ResultSet rs, String strColName) throws SQLException {
        byte nValue = rs.getByte(strColName);
        return rs.wasNull() ? null : nValue;
    }
}
