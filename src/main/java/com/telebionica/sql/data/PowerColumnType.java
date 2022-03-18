/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.data;

import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.util.JDBCUtil;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

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

    public void powerStatement(PreparedStatement pstm, int i) throws SQLException {
        if (columnType.getScale() == null) {
            pstm.setObject(i++, value, columnType.getType());
        } else {
            pstm.setObject(i++, value, columnType.getType(), columnType.getScale());
        }
    }

    public boolean push(Object target, ResultSet rs) throws QueryBuilderException, SQLException {

        Object any = null;
        try {
            Method m = getWriteMethod();
            if (columnType.getFieldClass().isAssignableFrom(Long.class)) {
                Long obj = JDBCUtil.getLong(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Integer.class)) {
                Integer obj = JDBCUtil.getInteger(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Float.class)) {
                Float obj = JDBCUtil.getFloat(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Double.class)) {
                Double obj = JDBCUtil.getDouble(rs, columnAlias);
                m.invoke(target, obj);
                any = obj;
            } else if (columnType.getFieldClass().isAssignableFrom(Boolean.class)) {
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
            } else {
                throw new QueryBuilderException("No hay definido un mapa de conversion de esta clase " + columnType.getFieldClass() + " " + columnType.getColumnName());
            }

        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }

        return any != null;
    }

    public Method getWriteMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(columnType.getTableType().getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(columnType.getFieldName())) {
                return pd.getWriteMethod();
            }
        }
        return null;
    }

    public Method getReadMethod() throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(columnType.getTableType().getEntityClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(columnType.getFieldName())) {
                return pd.getReadMethod();
            }
        }
        return null;
    }

    public void getter(Object obj) throws QueryBuilderException {
        try {
            Method m = getReadMethod();
            this.value = m.invoke(obj);
        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }
    }
}
