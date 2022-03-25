/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.type.ManyToOneType;
import com.telebionica.sql.type.TableType;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class JoinNode {

    private String alias;
    private ManyToOneType manyToOneType;
    private TableType childTableType;
    private List<PowerColumnType> selectColumns = new ArrayList();
    private Query.JOINTYPE joinType;
    private List<JoinNode> children = new ArrayList<>();

    public JoinNode(String alias, ManyToOneType manyToOneType) {
        this.alias = alias;
        this.manyToOneType = manyToOneType;
    }

    public String getFieldName() {
        return manyToOneType.getFieldName();
    }

    public String getAlias() {
        return alias;
    }

    public ManyToOneType getManyToOneType() {
        return manyToOneType;
    }

    public TableType getChildTableType() {
        return childTableType;
    }

    public void setChildTableType(TableType childTableType) {
        this.childTableType = childTableType;
    }

    public Query.JOINTYPE getJoinType() {
        return joinType;
    }

    public void setJoinType(Query.JOINTYPE joinType) {
        this.joinType = joinType;
    }

    public List<JoinColumn> getJoiners() {
        return manyToOneType.getJoiners();
    }

    public List<JoinNode> getChildren() {
        return children;
    }

    public void setChildren(List<JoinNode> children) {
        this.children = children;
    }

    public List<PowerColumnType> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(List<PowerColumnType> selectColumns) {
        this.selectColumns = selectColumns;
    }

    public Object newChild() throws QueryBuilderException {
        try {
            return getChildTableType().getEntityClass().getConstructor().newInstance();
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
            Logger.getLogger(JoinNode.class.getName()).log(Level.SEVERE, null, ex);
            throw new QueryBuilderException(ex);
        }
    }

    public void setter(Object obj, Object child) throws QueryBuilderException {
       manyToOneType.setter(obj, child);
    }
    
}
