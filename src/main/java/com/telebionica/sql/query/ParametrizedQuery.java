/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.data.PowerColumnType;
import java.util.List;
import java.util.ArrayList;
/**
 *
 * @author aldo
 */
public class ParametrizedQuery {
    
    private String query;
    private List<PowerColumnType> params = new ArrayList();
    private List<PowerColumnType> selectColumns;
    private List<JoinNode> joinNodes;

    public ParametrizedQuery(String query, List<PowerColumnType> params, List<PowerColumnType> selectColumns, List<JoinNode> joinNodes) {
        this.query = query;
        this.params = params;
        this.selectColumns = selectColumns;
        this.joinNodes = joinNodes;
    }
    
    public ParametrizedQuery(String query, List<PowerColumnType> params, List<JoinNode> joinNodes) {
        this.query = query;
        this.params = params;
        this.joinNodes = joinNodes;
    }

    public String getQuery() {
        return query;
    }

    public List<PowerColumnType> getParams() {
        return params;
    }

    public List<PowerColumnType> getSelectColumns() {
        return selectColumns;
    }

    public List<JoinNode> getJoinNodes() {
        return joinNodes;
    }
    
}
