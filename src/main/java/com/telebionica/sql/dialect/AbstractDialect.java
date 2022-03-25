/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.dialect;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.power.AbstractPowerManager;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.ManyToOneType;
import com.telebionica.sql.type.TableType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public abstract class AbstractDialect {

    protected final AbstractPowerManager pm;

    public AbstractDialect(AbstractPowerManager pm) {
        this.pm = pm;
    }

    public abstract ParametrizedQuery dryRun(Query query, boolean count, Connection conn) throws SQLException, QueryBuilderException;

    
    public ParametrizedQuery dryRun(Query query, boolean count) throws SQLException, QueryBuilderException {
        try(Connection conn = pm.getConnection()) {
            return dryRun(query, count, conn);
        } 
    }

    protected String joins(String alias, List<JoinNode> nodes, Query query) {

        StringBuilder sb = new StringBuilder();

        for (JoinNode node : nodes) {

            sb.append("\n");

            sb.append(node.getJoinType().getStatementJoin()).append(" ");

            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }

            sb.append(node.getChildTableType().getName()).append(" ");
            sb.append(node.getAlias()).append(" ON ");

            List<JoinColumn> cols = node.getJoiners();
            Iterator<JoinColumn> colIt = cols.iterator();
            while (colIt.hasNext()) {
                JoinColumn jc = colIt.next();

                sb.append(alias).append(".").append(jc.name());
                sb.append(" = ");
                sb.append(node.getAlias()).append(".").append(jc.referencedColumnName());

                if (colIt.hasNext()) {
                    sb.append(" AND ");
                }
            }

            sb.append(joins(node.getAlias(), node.getChildren(), query));
        }

        return sb.toString();
    }

    protected List<PowerColumnType> selectIndexator(Query query, TableType tableType) {

        List<PowerColumnType> selectColumns = new ArrayList();

        if (query.getQtype() == Query.QTYPE.SELECT) {

            List<ColumnType> selects = tableType.getFilterColumns(query.getSelectFieldNames());

            selectColumns = selects.stream().map(e -> {
                PowerColumnType sct = new PowerColumnType(e);
                sct.setColumnAlias(String.format("%s_%s", query.getRootAlias(), e.getColumnName()));
                return sct;
            }).collect(Collectors.toList());
        }

        return selectColumns;
    }

    protected String selectJoins(List<JoinNode> nodes) {
        StringBuilder sb = new StringBuilder();

        for (JoinNode node : nodes) {

            List<PowerColumnType> cols = node.getSelectColumns();

            if (cols.size() > 0) {
                sb.append(", \n");
            }

            Iterator<PowerColumnType> selectIt = cols.iterator();
            while (selectIt.hasNext()) {
                PowerColumnType selColumnType = selectIt.next();

                sb.append(node.getAlias()).append(".");
                sb.append(selColumnType.getColumnType().getColumnName());
                sb.append(" as ").append(selColumnType.getColumnAlias());

                if (selectIt.hasNext()) {
                    sb.append(", ");
                }
            }

            sb.append(selectJoins(node.getChildren()));
        }

        return sb.toString();
    }

    protected String limit(String queryString, Query query) {

        if (query.getQtype() == Query.QTYPE.SELECT) {

            if (query.getFirstResult() == null && query.getMaxResults() == null) {
                return queryString;
            }

            if (query.getFirstResult() == null && query.getMaxResults() != null) {
                return String.format("%s\nLIMIT %d", queryString, query.getMaxResults());
            }

            if (query.getFirstResult() != null && query.getMaxResults() != null) {
                return String.format("%s\nLIMIT %d, %d", queryString, query.getFirstResult(), query.getMaxResults());
            }
        }

        return "";
    }

    
}
