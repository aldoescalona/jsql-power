/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.AliasColumnType;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.TableType;
import com.telebionica.sql.data.ParamColumnType;
import com.telebionica.sql.data.SelectColumnType;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class Query<E> {

    private String schema;
    private String rootAlias;
    private TableType tableType;
    private AbstractQueryBuilder queryBuilder;
    private String[] selectFieldNames;
    private String[] deleteAliases;
    private Class<E> entityClass;

    private List<JoinNode> rootJoinNodes = new ArrayList();
    private List<Order> orders = new ArrayList();

    private Integer firstResult;
    private Integer maxResults;

    private QTYPE qtype;

    private List<ParamColumnType> params = new ArrayList<ParamColumnType>();
    private List<Predicate> predicates = new ArrayList();
    private List<SelectColumnType> selecTColumns = new ArrayList();

    public Query(AbstractQueryBuilder queryBuilder) throws SQLException, QueryBuilderException {
        this.queryBuilder = queryBuilder;
    }

    public Query schema(String schema) throws SQLException, QueryBuilderException {
        this.schema = schema;
        return this;
    }

    public Query<E> select(String... columnNames) throws SQLException, QueryBuilderException {
        qtype = QTYPE.SELECT;
        this.selectFieldNames = columnNames;
        return this;
    }

    public Query<E> from(Class<E> entityClass) throws SQLException, QueryBuilderException {
        return from(entityClass, "e");
    }

    public Query<E> from(Class<E> entityClass, String alias) throws SQLException, QueryBuilderException {
        this.rootAlias = alias;
        this.entityClass = entityClass;
        tableType = queryBuilder.getTableType(entityClass);
        return this;
    }

    public Query<E> join(String fieldPath, String alias) throws QueryBuilderException, SQLException {
        return join(fieldPath, alias, JOINTYPE.INNER);
    }

    public Query<E> left(String field, String alias) throws QueryBuilderException, SQLException {
        return join(field, alias, JOINTYPE.LEFT);
    }

    public Query<E> right(String field, String alias) throws QueryBuilderException, SQLException {
        return join(field, alias, JOINTYPE.RIGHT);
    }

    public Query<E> join(String fieldPath, String alias, JOINTYPE joinYype) throws QueryBuilderException, SQLException {

        String[] path = fieldPath.split("[.]");
        if (path.length <= 0) {
            return this;
            // throw new QueryBuilderException("No se encuentra el path " + fieldPath);
        }
        queryBuilder.joins(path, alias, rootJoinNodes, entityClass, joinYype);

        Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        String json = gson.toJson(rootJoinNodes);
        System.out.println(" JOINS: " + json);

        return this;
    }

    public Query<E> where(Predicate predicate) {
        predicate.setQuery(this);
        predicates.add(predicate);
        return this;
    }

    public Query<E> and(Predicate predicate) {
        predicate.setQuery(this);
        predicates.add(predicate);
        return this;
    }

    public Query addOrder(Order order) {
        order.setQuery(this);
        orders.add(order);
        return this;
    }

    public int insert(E e) throws SQLException, QueryBuilderException {

        this.entityClass = (Class<E>) e.getClass();
        tableType = queryBuilder.getTableType(entityClass);

        List<ColumnType> selects = tableType.getColumns();

        return 0;
    }

    public int update(E e, String... fields) {
        return 0;
    }

    public int delete(E e) {

        return 0;
    }

    public Query delete(String... aliases) {
        qtype = QTYPE.DELETE;
        deleteAliases = aliases;
        return this;
    }

    public List<E> list() throws Exception {
        List<E> list = new ArrayList();

        String query = dryRun(false);
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (ParamColumnType val : params) {
                if (val.getColumnType().getScale() == null) {
                    pstm.setObject(i++, val.getValue());
                } else {
                    pstm.setObject(i++, val.getValue(), val.getColumnType().getScale());
                }
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    E rootInstance = entityClass.getConstructor().newInstance();
                    for (SelectColumnType st : selecTColumns) {
                        st.push(rootInstance, rs);
                    }
                    push(rootInstance, rootJoinNodes, rs);
                    list.add(rootInstance);
                }
            }
        }

        // Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        // String json = gson.toJson(list);
        // System.out.println(" LIST: " + json);
        return list;
    }

    public Integer count() throws SQLException, QueryBuilderException {

        int c = 0;

        String query = dryRun(true);
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (ParamColumnType val : params) {
                if (val.getColumnType().getScale() == null) {
                    pstm.setObject(i++, val.getValue());
                } else {
                    pstm.setObject(i++, val.getValue(), val.getColumnType().getScale());
                }
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                if (rs.next()) {
                    c = rs.getInt(1);
                }
            }
        }

        return c;
    }

    public int execute() throws Exception {

        int c;
        String query = dryRun(false);
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (ParamColumnType val : params) {

                if (val.getColumnType().getScale() == null) {
                    pstm.setObject(i++, val.getValue());
                } else {
                    pstm.setObject(i++, val.getValue(), val.getColumnType().getScale());
                }
            }

            System.out.println(" R: ");
            c = pstm.executeUpdate();

        }

        // Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        // String json = gson.toJson(list);
        // System.out.println(" LIST: " + json);
        return c;
    }

    private void push(Object parent, List<JoinNode> nodes, ResultSet rs) throws Exception {

        for (JoinNode node : nodes) {

            boolean existe = false;
            List<SelectColumnType> cols = node.getSelectColumns();
            Object instance = node.getTableType().getEntityClass().getConstructor().newInstance();
            for (SelectColumnType st : cols) {
                existe = existe || st.push(instance, rs);
            }
            if (existe) {
                node.push(parent, instance);
                push(instance, node.getChildren(), rs);
            }
        }

    }

    public E unique() {
        return (E) new Object();
    }

    private void selectColumnsIndexar() {

        if (qtype == QTYPE.SELECT) {

            List<ColumnType> selects;

            if (selectFieldNames == null || selectFieldNames.length == 0) {
                selects = tableType.getColumns();
            } else {
                List<ColumnType> ids = tableType.getIdColumns();
                List<String> scls = Arrays.asList(selectFieldNames);
                List<ColumnType> cols = scls.stream().map(n -> tableType.getFieldColumnType(n)).collect(Collectors.toList());
                cols.removeAll(ids);
                selects = new ArrayList();
                selects.addAll(ids);
                selects.addAll(cols);
            }

            selecTColumns = selects.stream().map(e -> {
                SelectColumnType sct = new SelectColumnType(String.format("%s_%s", rootAlias, e.getColumnName()), e);
                return sct;
            }).collect(Collectors.toList());
        }
    }

    public String dryRun(boolean count) throws SQLException, QueryBuilderException {

        params.clear();

        if (tableType == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        if (qtype == QTYPE.SELECT) {

            sb.append("SELECT ");
            if (!count) {

                selectColumnsIndexar();

                Iterator<SelectColumnType> selectIt = selecTColumns.iterator();
                while (selectIt.hasNext()) {
                    SelectColumnType selColumnType = selectIt.next();

                    sb.append(rootAlias).append(".");
                    sb.append(selColumnType.getColumnType().getColumnName());
                    sb.append(" as ").append(selColumnType.getQueryKey());

                    if (selectIt.hasNext()) {
                        sb.append(", ");
                    }
                }

                sb.append(selectJoins(rootJoinNodes));
            } else {
                sb.append("COUNT(*)");
            }

        } else if (qtype == QTYPE.DELETE) {

            sb.append("DELETE ");

            List<String> deletes = new ArrayList();
            if (deleteAliases == null || deleteAliases.length == 0) {
                deletes.add(rootAlias);
            } else {
                deletes.addAll(Arrays.asList(deleteAliases));
            }

            Iterator<String> delIt = deletes.iterator();

            while (delIt.hasNext()) {
                String del = delIt.next();
                sb.append(del);
                if (delIt.hasNext()) {
                    sb.append(",");
                }
            }
        }

        String joiners = joins(rootAlias, rootJoinNodes);

        sb.append("\nFROM ");

        if (schema != null) {
            sb.append(schema).append(".");
        }
        sb.append(tableType.getName()).append(" ").append(rootAlias).append(" ");

        sb.append(joiners);

        if (predicates.size() > 0) {
            sb.append("\nWHERE ");

            Iterator<Predicate> it = predicates.iterator();
            while (it.hasNext()) {
                Predicate p = it.next();
                p.build();

                sb.append(p.getPredicateStatement());
                params.addAll(p.getValueTypes());

                if (it.hasNext()) {
                    sb.append(" AND ");
                }
            }
        }

        System.out.println(" VALUES: " + params);

        if (qtype == QTYPE.SELECT && !count) {

            if (orders.size() > 0) {
                sb.append("\nORDER BY ");
                Iterator<Order> it = orders.iterator();
                while (it.hasNext()) {
                    Order order = it.next();
                    order.build();
                    sb.append(order.getOrderStatement());

                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            sb.append(limit());
        }

        System.out.println(" QUERY: " + sb.toString());

        return sb.toString();
    }

    private String selectJoins(List<JoinNode> nodes) {
        StringBuilder sb = new StringBuilder();

        for (JoinNode node : nodes) {

            List<SelectColumnType> cols = node.getSelectColumns();

            if (cols.size() > 0) {
                sb.append(", \n");
            }

            Iterator<SelectColumnType> selectIt = cols.iterator();
            while (selectIt.hasNext()) {
                SelectColumnType selColumnType = selectIt.next();

                sb.append(node.getAlias()).append(".");
                sb.append(selColumnType.getColumnType().getColumnName());
                sb.append(" as ").append(selColumnType.getQueryKey());

                if (selectIt.hasNext()) {
                    sb.append(", ");
                }
            }

            sb.append(selectJoins(node.getChildren()));
        }

        return sb.toString();
    }

    private String joins(String alias, List<JoinNode> nodes) {

        StringBuilder sb = new StringBuilder();

        for (JoinNode node : nodes) {

            sb.append("\n");

            sb.append(node.getJoinType().getStatementJoin()).append(" ");

            if (schema != null) {
                sb.append(schema).append(".");
            }

            sb.append(node.getTableType().getName()).append(" ");
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

            sb.append(joins(node.getAlias(), node.getChildren()));
        }

        return sb.toString();
    }

    private String limit() {

        if (qtype == QTYPE.SELECT) {

            if (firstResult == null && maxResults == null) {
                return "";
            }

            if (firstResult == null && maxResults != null) {
                return String.format("\nLIMIT %d", maxResults);
            }

            if (firstResult != null && maxResults != null) {
                return String.format("\nLIMIT %d, %d", firstResult, maxResults);
            }
        }

        return "";
    }

    public Integer getFirstResult() {
        return firstResult;
    }

    public Query setFirstResult(Integer firstResult) {
        this.firstResult = firstResult;
        return this;
    }

    public Integer getMaxResults() {
        return maxResults;
    }

    public Query setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
        return this;
    }
    

    public AliasColumnType getAliasColumnType(String fieldPath) {
        String[] path = fieldPath.split("[.]");
        if (path.length <= 0) {
            return null;
        }

        if (path.length == 1) {
            ColumnType ct = tableType.getFieldColumnType(path[0]);
            if (ct == null) {
                return null;
            }

            AliasColumnType ext = new AliasColumnType(null, ct);
            return ext;

        }

        String alias = path[0];
        String field = path[1];

        if (alias.equals(rootAlias)) {
            ColumnType ct = tableType.getFieldColumnType(field);
            if (ct == null) {
                return null;
            }
            AliasColumnType ext = new AliasColumnType(alias, ct);
            return ext;
        }

        ColumnType ct = find(alias, field, rootJoinNodes);
        if (ct == null) {
            return null;
        }

        AliasColumnType ext = new AliasColumnType(alias, ct);
        return ext;
    }

    private ColumnType find(String alias, String field, List<JoinNode> nodes) {

        ColumnType ct = null;

        for (JoinNode node : nodes) {
            if (node.getAlias().equals(alias)) {
                ct = node.getTableType().getFieldColumnType(field);
                return ct;
            }
            if (node.getChildren().size() > 0) {
                ct = find(alias, field, node.getChildren());
            }
        }
        return ct;
    }

    public static enum QTYPE {
        SELECT, UPDATE, INSERT, DELETE;
    }

    public static enum COUN_TYPE {
        COUNT_ALL(false), COUNT_LIMIT(true);

        private COUN_TYPE(boolean limit) {
            this.limit = limit;
        }

        public boolean isLimit() {
            return limit;
        }

        private boolean limit;
    }

    public static enum JOINTYPE {
        INNER("INNER JOIN"), LEFT("LEFT OUTER JOIN"), RIGHT("RIGHT OUTER JOIN");

        private JOINTYPE(String statementJoin) {
            this.statementJoin = statementJoin;
        }

        public String getStatementJoin() {
            return statementJoin;
        }

        private String statementJoin;
    }

}
