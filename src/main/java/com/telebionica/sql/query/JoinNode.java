/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.data.SelectColumnType;
import com.telebionica.sql.type.TableType;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class JoinNode {
    
    private String fieldName;
    private String alias;
    private TableType tableType;
    private List<SelectColumnType> selectColumns = new ArrayList();
    private Query.JOINTYPE joinType;
    protected List<JoinColumn> joiners;
    private List<JoinNode> children = new ArrayList<>();

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    public Query.JOINTYPE getJoinType() {
        return joinType;
    }

    public void setJoinType(Query.JOINTYPE joinType) {
        this.joinType = joinType;
    }

    public List<JoinColumn> getJoiners() {
        return joiners;
    }

    public void setJoiners(List<JoinColumn> joiners) {
        this.joiners = joiners;
    }

    public List<JoinNode> getChildren() {
        return children;
    }

    public void setChildren(List<JoinNode> children) {
        this.children = children;
    }

    public List<SelectColumnType> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(List<SelectColumnType> selectColumns) {
        this.selectColumns = selectColumns;
    }
    
    public void push(Object obj, Object child) throws Exception{
        Method m = getWriteMethod(obj.getClass(), getFieldName());
        m.invoke(obj, child);
    }
    
    
    public Method getWriteMethod(Class clazz, String propertyName) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(propertyName)) {
                return pd.getWriteMethod();
            }
        }
        return null;
    }
}
