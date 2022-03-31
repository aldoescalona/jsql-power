/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.data;

import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.util.JDBCUtil;
import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author aldo
 */
public class PowerColumnType {

    private ColumnType columnType;
    private Object value;
    private String tableAlias;
    private String columnAlias;

    public PowerColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public PowerColumnType(ColumnType columnType, Object value) {
        this.columnType = columnType;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }

    public String getFullColumnName() {
        if (tableAlias == null) {
            return columnType.getColumnName();
        } else {
            return String.format("%s.%s", tableAlias, columnType.getColumnName());
        }
    }

    public void powerStatement(PreparedStatement pstm, int i) throws SQLException, QueryBuilderException {

        if (columnType.hasEnumerated() && value != null && value.getClass().isEnum()) {
            try {
                Method m = columnType.getFieldClass().getMethod("ordinal");
                Integer ordinal = (Integer) m.invoke(value);
                pstm.setObject(i++, ordinal, columnType.getType());
            } catch (Exception e) {
                throw new QueryBuilderException("Conversion de dato ", e);
            }
            return;
        }

        if (columnType.getScale() == null) {
            pstm.setObject(i++, value, columnType.getType());
        } else {
            pstm.setObject(i++, value, columnType.getType(), columnType.getScale());
        }
    }

    public boolean push(Object target, ResultSet rs) throws QueryBuilderException, SQLException {

        Object any = null;
        try {
            Method m = columnType.getWriteMethod();
            if (columnType.getFieldClass().isAssignableFrom(Long.class) || columnType.getFieldClass().isAssignableFrom(long.class)) {
                Long obj = JDBCUtil.getLong(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Integer.class) || columnType.getFieldClass().isAssignableFrom(int.class)) {
                Integer obj = JDBCUtil.getInteger(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Float.class) || columnType.getFieldClass().isAssignableFrom(float.class)) {
                Float obj = JDBCUtil.getFloat(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Double.class) || columnType.getFieldClass().isAssignableFrom(double.class)) {
                Double obj = JDBCUtil.getDouble(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Boolean.class) || columnType.getFieldClass().isAssignableFrom(boolean.class)) {
                Boolean obj = JDBCUtil.getBoolean(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(String.class)) {
                String obj = rs.getString(columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(BigDecimal.class)) {
                BigDecimal obj = rs.getBigDecimal(columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Date.class)) {
                Date obj = rs.getTimestamp(columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.hasEnumerated() && columnType.getFieldClass().isEnum()) {
                Integer ordinal = JDBCUtil.getInteger(rs, columnAlias);
                if (ordinal != null) {
                    Object[] enus = columnType.getFieldClass().getEnumConstants();
                    Object obj = enus[ordinal];
                    m.invoke(target, obj);
                    any = obj;
                }
            } else {
                throw new QueryBuilderException("No hay definido un mapa de conversion de esta clase " + columnType.getFieldClass() + " " + columnType.getColumnName());
            }

        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }

        return any != null;
    }

    public boolean push(Object target, ResultSet rs, int i) throws QueryBuilderException, SQLException {

        Object any = null;
        try {
            Method m = columnType.getWriteMethod();
            if (columnType.getFieldClass().isAssignableFrom(Long.class) || columnType.getFieldClass().isAssignableFrom(long.class)) {
                Long obj = JDBCUtil.getLong(rs, i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Integer.class) || columnType.getFieldClass().isAssignableFrom(int.class)) {
                Integer obj = JDBCUtil.getInteger(rs, i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Float.class) || columnType.getFieldClass().isAssignableFrom(float.class)) {
                Float obj = JDBCUtil.getFloat(rs, i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Double.class) || columnType.getFieldClass().isAssignableFrom(double.class)) {
                Double obj = JDBCUtil.getDouble(rs, i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Boolean.class) || columnType.getFieldClass().isAssignableFrom(boolean.class)) {
                Boolean obj = JDBCUtil.getBoolean(rs, i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(String.class)) {
                String obj = rs.getString(i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(BigDecimal.class)) {
                BigDecimal obj = rs.getBigDecimal(i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Date.class)) {
                Date obj = rs.getTimestamp(i);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.hasEnumerated() && columnType.getFieldClass().isEnum()) {
                Integer ordinal = JDBCUtil.getInteger(rs, i);
                if (ordinal != null) {
                    Object[] enus = columnType.getFieldClass().getEnumConstants();
                    Object obj = enus[ordinal];
                    m.invoke(target, obj);
                    any = obj;
                }
            } else {
                throw new QueryBuilderException("No hay definido un mapa de conversion de esta clase " + columnType.getFieldClass() + " " + columnType.getColumnName());
            }

        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }

        return any != null;
    }

    public void setter(Object obj, Object child) throws QueryBuilderException {
        try {
            Method m = columnType.getWriteMethod();
            m.invoke(obj, child);
        } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException | IntrospectionException ex) {
            Logger.getLogger(JoinNode.class.getName()).log(Level.SEVERE, null, ex);
            throw new QueryBuilderException(ex);
        }
    }

    public void getter(Object obj) throws QueryBuilderException {
        try {
            Method m = columnType.getReadMethod();
            this.value = m.invoke(obj);
        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }
    }
}
