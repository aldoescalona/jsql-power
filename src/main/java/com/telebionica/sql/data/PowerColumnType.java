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
import java.sql.ResultSet;
import java.util.Date;

/**
 *
 * @author aldo
 */
public class PowerColumnType {

    private ColumnType columnType;
    private Object value;
    private String alias;
    private String selectKey;
    
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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getSelectKey() {
        return selectKey;
    }

    public void setSelectKey(String selectKey) {
        this.selectKey = selectKey;
    }

    public String getFullColumnName() {
        if (alias == null) {
            return columnType.getColumnName();
        } else {
            return String.format("%s.%s", alias, columnType.getColumnName());
        }
    }

    public boolean push(Object target, ResultSet rs) throws Exception {

        Object any = null;
        
        Method m = getWriteMethod();
        if (columnType.getFieldClass().isAssignableFrom(Long.class)) {
            Long obj = JDBCUtil.getLong(rs, selectKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Integer.class)){
            Integer obj = JDBCUtil.getInteger(rs, selectKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Boolean.class)){
            Boolean obj = JDBCUtil.getBoolean(rs, selectKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(String.class)){
            String obj = rs.getString(selectKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(BigDecimal.class)){
            BigDecimal obj = rs.getBigDecimal(selectKey);
            m.invoke(target, obj);
            any = obj;
        } else if(columnType.getFieldClass().isAssignableFrom(Date.class)){
            Date obj = rs.getTimestamp(selectKey);
            m.invoke(target, obj);
            any = obj;
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
