/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.data;

import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.util.JDBCUtil;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.Date;

/**
 *
 * @author aldo
 */
public class SelectColumnType {
    
    private String queryKey;
    private ColumnType columnType;

    public SelectColumnType(String queryKey, ColumnType columnType) {
        this.queryKey = queryKey;
        this.columnType = columnType;
    }
    

    public String getQueryKey() {
        return queryKey;
    }

    public void setQueryKey(String queryKey) {
        this.queryKey = queryKey;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }
    
    public boolean push(Object target, ResultSet rs) throws Exception {

        Object any = null;
        
        Method m = columnType.getWriteMethod();
        if (columnType.getFieldClass().isAssignableFrom(Long.class)) {
            Long obj = JDBCUtil.getLong(rs, queryKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Integer.class)){
            Integer obj = JDBCUtil.getInteger(rs, queryKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Boolean.class)){
            Boolean obj = JDBCUtil.getBoolean(rs, queryKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(String.class)){
            String obj = rs.getString(queryKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(BigDecimal.class)){
            BigDecimal obj = rs.getBigDecimal(queryKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Date.class)){
            Date obj = rs.getTimestamp(queryKey);
            m.invoke(target, obj);
            any = obj;
        }
        
        return any != null;
    }

    
}
