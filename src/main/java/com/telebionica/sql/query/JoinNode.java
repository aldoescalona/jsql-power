/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.type.ManyToOneType;
import com.telebionica.sql.type.TableType;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class JoinNode {
    
    private String alias;
    private ManyToOneType manyToOneType;
    private TableType tableType;
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
    
}
