/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.predicates.Predicate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.TableType;
import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.setu.SetForUpdate;
import com.telebionica.sql.type.ManyToOneType;
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

    private final AbstractQueryBuilder queryBuilder;
    private String schema;
    private String rootAlias;
    private TableType tableType;

    private String[] selectFieldNames;
    private String[] deleteAliases;

    private Class<E> entityClass;

    private List<JoinNode> rootJoinNodes;
    private List<Order> orders;

    private Integer firstResult;
    private Integer maxResults;

    private QTYPE qtype;

    private List<Predicate> predicates;
    private List<SetForUpdate> sets;

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

        if (qtype == null) {
            throw new QueryBuilderException("No se ha establecido el qtype, use previamente select() o delete(String ...aliases)");
        }

        if (alias == null) {
            throw new QueryBuilderException("Alias no puede ser nulo en from(Class entityClass, String alias)");
        }

        this.rootAlias = alias;
        this.entityClass = entityClass;
        this.tableType = queryBuilder.getTableType(entityClass);

        this.rootJoinNodes = new ArrayList();
        this.orders = new ArrayList();
        this.predicates = new ArrayList();

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

    public Query<E> join(String fieldPath, String alias, JOINTYPE jointype) throws QueryBuilderException, SQLException {

        if (rootAlias == null) {
            throw new QueryBuilderException("No se ha establecido un alias del root en en from(Class entityClass, String alias) ");
        }

        if (jointype == null) {
            throw new QueryBuilderException("Jointype no puede ser nulo");
        }

        String[] path = fieldPath.split("[.]");
        if (path.length <= 0) {
            // return this;
            throw new QueryBuilderException("No se encuentra el path " + fieldPath);
        }
        queryBuilder.joins(path, alias, rootJoinNodes, tableType, jointype);

        /* Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        String json = gson.toJson(rootJoinNodes);
        System.out.println(" JOINS: " + json); */

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

    public Query set(SetForUpdate set) {
        set.setQuery(this);
        sets.add(set);
        return this;
    }

    public int insert(E e) throws SQLException, QueryBuilderException {

        this.entityClass = (Class<E>) e.getClass();
        tableType = queryBuilder.getTableType(entityClass);

        List<ColumnType> cols = tableType.getColumns();

        List<PowerColumnType> insertParams = new ArrayList();
        for (ColumnType ct : cols) {
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(ct.getColumnName());
            param.getter(e);
            insertParams.add(param);
        }

        List<ManyToOneType> m2otList = tableType.getManyToOnes();
        for (ManyToOneType m2o : m2otList) {

            Object obj = m2o.getter(e);
            if (obj == null) {
                continue;
            }

            TableType otherTT = queryBuilder.getTableType(m2o.getFieldClass());

            List<JoinColumn> jcs = m2o.getJoiners();
            for (JoinColumn jc : jcs) {
                String columName = jc.referencedColumnName();
                ColumnType ct = otherTT.getColumnType(columName);

                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(jc.name());
                param.getter(obj);
                insertParams.add(param);
            }
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder sbvalues = new StringBuilder();
        sb.append("INSERT INTO ");

        if (schema != null) {
            sb.append(schema).append(".");
        }

        sb.append(tableType.getName());
        sb.append(" (");

        Iterator<PowerColumnType> colIt = insertParams.iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            sb.append(ct.getColumnAlias());
            sbvalues.append("?");

            if (colIt.hasNext()) {
                sb.append(", ");
                sbvalues.append(", ");
            }
        }

        sb.append(" ) VALUES(");
        sb.append(sbvalues);
        sb.append(")");

        String query = sb.toString();

        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : insertParams) {
                powerValue.powerStatement(pstm, i++);
            }
            pstm.executeUpdate();
        }

        System.out.println(" INSERT " + sb);

        return 0;
    }

    public int update(E e, String... fields) throws SQLException, QueryBuilderException {

        this.entityClass = (Class<E>) e.getClass();
        tableType = queryBuilder.getTableType(entityClass);

        List<ColumnType> cols = tableType.getFilterColumns(fields);

        List<PowerColumnType> updateParams = new ArrayList();
        for (ColumnType ct : cols) {
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(ct.getColumnName());
            param.getter(e);
            updateParams.add(param);
        }

        List<ManyToOneType> m2otList = tableType.getManyToOnes();
        for (ManyToOneType m2o : m2otList) {

            Object obj = m2o.getter(e);
            if (obj == null) {
                continue;
            }

            TableType otherTT = queryBuilder.getTableType(m2o.getFieldClass());

            List<JoinColumn> jcs = m2o.getJoiners();
            for (JoinColumn jc : jcs) {
                String columName = jc.referencedColumnName();
                ColumnType ct = otherTT.getColumnType(columName);

                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(jc.name());
                param.getter(obj);
                updateParams.add(param);
            }
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder sbvalues = new StringBuilder();
        sb.append("UPDATE ");

        if (schema != null) {
            sb.append(schema).append(".");
        }

        sb.append(tableType.getName());
        sb.append(" SET ");

        List<PowerColumnType> orderParams = new ArrayList();
        Iterator<PowerColumnType> colIt = updateParams.stream().filter(c -> !c.getColumnType().isPrimary()).collect(Collectors.toList()).iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            if (!ct.getColumnType().isPrimary()) {
                sb.append(ct.getColumnAlias());
                sb.append(" = ?");
                orderParams.add(ct);
                if (colIt.hasNext()) {
                    sb.append(", ");

                }
            }
        }

        colIt = updateParams.stream().filter(c -> c.getColumnType().isPrimary()).collect(Collectors.toList()).iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            if (ct.getColumnType().isPrimary()) {
                sbvalues.append(ct.getColumnAlias());
                sbvalues.append(" = ?");
                orderParams.add(ct);
                if (colIt.hasNext()) {
                    sbvalues.append(" AND ");

                }
            }
        }

        sb.append(" WHERE ");
        sb.append(sbvalues);

        String query = sb.toString();

        System.out.println(" UPDATE QUERY: " + query);
        int c = 0;
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : orderParams) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        }

        return c;
    }

    public Query<E> update(Class<E> entityClass, String alias) throws SQLException, QueryBuilderException {

        if (qtype != null && (qtype == QTYPE.SELECT || qtype == QTYPE.INSERT || qtype == QTYPE.DELETE)) {
            throw new QueryBuilderException("Ya se ha fijado el query como " + qtype + " elija oportunamente un tipo");
        }

        qtype = QTYPE.UPDATE;

        this.rootAlias = alias;
        this.entityClass = entityClass;
        this.tableType = queryBuilder.getTableType(entityClass);

        this.rootJoinNodes = new ArrayList();
        this.orders = new ArrayList();
        this.predicates = new ArrayList();
        this.sets = new ArrayList<>();
        return this;
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

        ParametrizedQuery pq = dryRun(false);
        String query = pq.getQuery();

        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    E rootInstance = entityClass.getConstructor().newInstance();
                    for (PowerColumnType st : pq.getSelectColumns()) {
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

        ParametrizedQuery pq = dryRun(true);
        String query = pq.getQuery();
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
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
        ParametrizedQuery pq = dryRun(false);
        String query = pq.getQuery();
        try ( Connection conn = queryBuilder.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        }

        return c;
    }

    private void push(Object parent, List<JoinNode> nodes, ResultSet rs) throws QueryBuilderException, SQLException {

        for (JoinNode node : nodes) {

            boolean any = false;
            List<PowerColumnType> cols = node.getSelectColumns();
            Object child = node.newChild();
            for (PowerColumnType st : cols) {
                any = any || st.push(child, rs);
            }
            if (any) {
                node.setter(parent, child);
                push(child, node.getChildren(), rs);
            }
        }

    }

    public E unique() {
        return (E) new Object();
    }

    private List<PowerColumnType> selectIndexator() {

        List<PowerColumnType> selectColumns = new ArrayList();

        if (qtype == QTYPE.SELECT) {

            List<ColumnType> selects = tableType.getFilterColumns(selectFieldNames);

            selectColumns = selects.stream().map(e -> {
                PowerColumnType sct = new PowerColumnType(e);
                sct.setColumnAlias(String.format("%s_%s", rootAlias, e.getColumnName()));
                return sct;
            }).collect(Collectors.toList());
        }

        return selectColumns;
    }

    public ParametrizedQuery dryRun(boolean count) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        String query = "";

        StringBuilder sb = new StringBuilder();
        String joiners = joins(rootAlias, rootJoinNodes);

        if (qtype == QTYPE.SELECT) {

            sb.append("SELECT ");
            if (!count) {

                selectColumns = selectIndexator();

                Iterator<PowerColumnType> selectIt = selectColumns.iterator();
                while (selectIt.hasNext()) {
                    PowerColumnType selColumnType = selectIt.next();

                    sb.append(rootAlias).append(".");
                    sb.append(selColumnType.getColumnType().getColumnName());
                    sb.append(" as ").append(selColumnType.getColumnAlias());

                    if (selectIt.hasNext()) {
                        sb.append(", ");
                    }
                }

                sb.append(selectJoins(rootJoinNodes));
            } else {
                sb.append("COUNT(*)");
            }

            sb.append("\nFROM ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(rootAlias).append(" ");
            sb.append(joiners);

            StringBuilder wheresb = new StringBuilder();
            if (predicates.size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> it = predicates.iterator();
                while (it.hasNext()) {
                    Predicate p = it.next();
                    p.build();

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (it.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb);

            if (count) {
                query = sb.toString();
            } else {
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

                query = sb.toString();
                query = limit(query);
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

            sb.append("\nFROM ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(rootAlias).append(" ");
            sb.append(joiners);

            StringBuilder wheresb = new StringBuilder();
            if (predicates.size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> it = predicates.iterator();
                while (it.hasNext()) {
                    Predicate p = it.next();
                    p.build();

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (it.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb);

            query = sb.toString();
        } else if (qtype == QTYPE.UPDATE) {
            sb.append("UPDATE ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(rootAlias).append(" ");

            sb.append(joiners);

            if (!sets.isEmpty()) {
                sb.append("\nSET ");
            }

            Iterator<SetForUpdate> it = sets.iterator();
            while (it.hasNext()) {
                SetForUpdate p = it.next();
                p.build();

                sb.append(p.getAsignStatement());
                if (p.hasValue()) {
                    params.add(p.getValueType());
                }

                if (it.hasNext()) {
                    sb.append(", ");
                }
            }

            StringBuilder wheresb = new StringBuilder();
            if (predicates.size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> itp = predicates.iterator();
                while (itp.hasNext()) {
                    Predicate p = itp.next();
                    p.build();

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (itp.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb.toString());
            query = sb.toString();
        }

        System.out.println(" QUERY: " + query);

        ParametrizedQuery pq = new ParametrizedQuery(query, params, selectColumns);
        return pq;
    }

    private String selectJoins(List<JoinNode> nodes) {
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

    private String joins(String alias, List<JoinNode> nodes) {

        StringBuilder sb = new StringBuilder();

        for (JoinNode node : nodes) {

            sb.append("\n");

            sb.append(node.getJoinType().getStatementJoin()).append(" ");

            if (schema != null) {
                sb.append(schema).append(".");
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

            sb.append(joins(node.getAlias(), node.getChildren()));
        }

        return sb.toString();
    }

    private String limit(String query) {

        if (qtype == QTYPE.SELECT) {

            if (firstResult == null && maxResults == null) {
                return query;
            }

            if (firstResult == null && maxResults != null) {
                return String.format("%s\nLIMIT %d", query, maxResults);
            }

            if (firstResult != null && maxResults != null) {
                return String.format("%s\nLIMIT %d, %d", query, firstResult, maxResults);
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

    public PowerColumnType getAliasColumnType(String fieldPath) {
        String[] path = fieldPath.split("[.]");
        if (path.length <= 0) {
            return null;
        }

        if (path.length == 1) {
            ColumnType ct = tableType.getFieldColumnType(path[0]);
            if (ct == null) {
                return null;
            }

            PowerColumnType ext = new PowerColumnType(ct);
            return ext;

        }

        String alias = path[0];
        String field = path[1];

        if (alias.equals(rootAlias)) {
            ColumnType ct = tableType.getFieldColumnType(field);
            if (ct == null) {
                return null;
            }
            PowerColumnType ext = new PowerColumnType(ct);
            ext.setTableAlias(alias);
            return ext;
        }

        ColumnType ct = find(alias, field, rootJoinNodes);
        if (ct == null) {
            return null;
        }

        PowerColumnType ext = new PowerColumnType(ct);
        ext.setTableAlias(alias);
        return ext;
    }

    private ColumnType find(String alias, String field, List<JoinNode> nodes) {

        ColumnType ct = null;

        for (JoinNode node : nodes) {
            if (node.getAlias().equals(alias)) {
                ct = node.getChildTableType().getFieldColumnType(field);
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
        INNER("INNER JOIN"), LEFT("LEFT JOIN"), RIGHT("RIGHT JOIN");

        private JOINTYPE(String statementJoin) {
            this.statementJoin = statementJoin;
        }

        public String getStatementJoin() {
            return statementJoin;
        }

        private final String statementJoin;
    }

}
