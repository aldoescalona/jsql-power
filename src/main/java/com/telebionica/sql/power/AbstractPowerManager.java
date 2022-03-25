/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.power;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.dialect.AbstractDialect;
import com.telebionica.sql.dialect.Dialect;
import com.telebionica.sql.join.Join;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.ManyToOneType;
import com.telebionica.sql.type.TableType;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.Table;

/**
 *
 * @author aldo
 */
public abstract class AbstractPowerManager {

    private String metadaSchema;
    private final Map<Class, TableType> tableTypeMap = new HashMap<>();
    private AbstractDialect dialect = null;

    public abstract Connection getConnection() throws SQLException;

    public <E> Query createQuery() throws SQLException, QueryBuilderException {
        Query<E> query = new Query(this);
        return query;
    }

    public int insert(Object e) throws SQLException, QueryBuilderException {
        return insert(null, e);
    }

    public int insert(String schema, Object e) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()){
            return insert(schema, e, conn);
        }
    }
    public int insert(String schema, Object e, Connection conn) throws SQLException, QueryBuilderException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

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

            TableType otherTT = this.getTableType(m2o.getFieldClass(), conn);

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

        try (PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : insertParams) {
                powerValue.powerStatement(pstm, i++);
            }
            pstm.executeUpdate();
        }

        System.out.println(" INSERT " + sb);

        return 0;
    }

    public int update(Object e, String... fields) throws SQLException, QueryBuilderException {
        return update(null, e, fields);
    }
    
    public int update(String schema,  Object e, String... fields) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return update(schema, e, conn, fields);
        } 
    }

    public int update(String schema, Object e, Connection conn, String... fields) throws SQLException, QueryBuilderException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

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

            TableType otherTT = this.getTableType(m2o.getFieldClass(), conn);

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
        try (PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : orderParams) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        }

        return c;
    }

    public int delete(Object e) {
        return 0;
    }

    public synchronized TableType getTableType(Class entityClass) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return getTableType(entityClass, conn);
        }
    }

    public synchronized TableType getTableType(Class entityClass, Connection conn) throws SQLException, QueryBuilderException {

        TableType tableType = tableTypeMap.get(entityClass);
        if (tableType == null) {
            tableType = buildTableType(entityClass, conn);
            tableTypeMap.put(entityClass, tableType);
        }
        return tableType;
    }

    private <E> TableType buildTableType(Class<E> entityClass, Connection conn) throws SQLException, QueryBuilderException {

        Table ann = (Table) entityClass.getDeclaredAnnotation(Table.class);
        if (ann == null) {
            throw new QueryBuilderException("No existe la anotacion @Table en la clase " + entityClass);
        }

        String tableNeme = ann.name();

        TableType tableType = new TableType(tableNeme, entityClass);

        List<ColumnType> columns = new ArrayList();
        List<ManyToOneType> manyToOneTypes = new ArrayList();

        Field[] scopeFields = entityClass.getDeclaredFields();

        for (Field field : scopeFields) {
            addColumnType(field, columns, tableType);
            addManyToOneType(field, manyToOneTypes, tableType);
        }

        StringBuilder sb = new StringBuilder("SELECT ");
        Iterator<ColumnType> colIt = columns.iterator();

        while (colIt.hasNext()) {
            ColumnType columnType = colIt.next();
            sb.append(columnType.getColumnName());
            if (colIt.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(" FROM ");

        if (metadaSchema != null) {
            sb.append(metadaSchema).append(".");
        }

        sb.append(tableNeme);
        sb.append(" WHERE 1 = 0 LIMIT 1");

        String query = sb.toString();

        try (PreparedStatement pst = conn.prepareStatement(query); ResultSet rs = pst.executeQuery()) {
            ResultSetMetaData mdrd = rs.getMetaData();

            for (int i = 0; i < columns.size(); i++) {
                int index = i + 1;

                ColumnType columnType = columns.get(i);

                String cname = mdrd.getColumnName(index);
                Integer ctype = mdrd.getColumnType(index);

                columnType.setColumnName(cname);
                columnType.setType(ctype);

                if (ctype == Types.DECIMAL) {
                    columnType.setScale(mdrd.getScale(index));
                }
            }
        }

        tableType.setColumns(columns);
        tableType.setManyToOnes(manyToOneTypes);

        Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        String json = gson.toJson(tableType);

        System.out.println("TablaType " + json);

        return tableType;
    }

    private void addColumnType(Field field, List<ColumnType> columns, TableType tableType) {
        Column colann = field.getAnnotation(Column.class);
        if (colann != null) {
            ColumnType ct = new ColumnType(colann.name(), field.getName(), field.getType(), tableType);
            Id idann = field.getAnnotation(Id.class);
            ct.setPrimary(idann != null);
            columns.add(ct);
        }
    }

    private void addManyToOneType(Field field, List<ManyToOneType> manyToOneTypes, TableType tableType) {
        JoinColumn jcann = field.getAnnotation(JoinColumn.class);
        JoinColumns jcanns = field.getAnnotation(JoinColumns.class);
        List<JoinColumn> joiners = null;
        if (jcann != null) {
            joiners = new ArrayList();
            joiners.add(jcann);
        }

        if (jcanns != null) {
            joiners = Arrays.asList(jcanns.value());
        }

        if (joiners != null && !joiners.isEmpty()) {
            ManyToOneType m2ot = new ManyToOneType(field.getName(), field.getType(), joiners, tableType);
            manyToOneTypes.add(m2ot);
        }
    }

    public AbstractDialect getDialect() throws QueryBuilderException {
        if (dialect == null && this.getClass().isAnnotationPresent(Dialect.class)) {
            try {

                Dialect ann = this.getClass().getAnnotation(Dialect.class);
                Class<? extends AbstractDialect> theclass = ann.dialectClass();
                dialect = theclass.getConstructor(AbstractPowerManager.class).newInstance(this);
            } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
                throw new QueryBuilderException("No se ha definido un dialect valido ", e);
            }
        }

        return dialect;
    }
    
    public <E> List<E> list(Query<E> query) throws QueryBuilderException, SQLException {
        try (Connection conn = getConnection()){
            return list(query, conn);
        } 
    }

    public <E> List<E> list(Query<E> query, Connection conn) throws QueryBuilderException, SQLException {
        List<E> list = new ArrayList();

        ParametrizedQuery pq = getDialect().dryRun(query, false, conn);
        String queryString = pq.getQuery();

        try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }

            try (ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    E rootInstance;
                    try {
                        rootInstance = query.getEntityClass().getConstructor().newInstance();
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                        Logger.getLogger(AbstractPowerManager.class.getName()).log(Level.SEVERE, null, ex);
                        throw new QueryBuilderException(" No se pudo crear instancia de la clase query.getEntityClass() ", ex);
                    }
                    for (PowerColumnType st : pq.getSelectColumns()) {
                        st.push(rootInstance, rs);
                    }
                    push(rootInstance, pq.getJoinNodes(), rs);
                    list.add(rootInstance);
                }
            }
        }

        // Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        // String json = gson.toJson(list);
        // System.out.println(" LIST: " + json);
        return list;
    }

    public Integer count(Query query) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()){
            return count(query, conn);
        } 
    }
    public Integer count(Query query, Connection conn) throws SQLException, QueryBuilderException {

        int c = 0;

        ParametrizedQuery pq = getDialect().dryRun(query, true, conn);
        String queryString = pq.getQuery();
        try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }

            try (ResultSet rs = pstm.executeQuery()) {
                if (rs.next()) {
                    c = rs.getInt(1);
                }
            }
        }

        return c;
    }
    
    public int execute(Query query) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()){
            return execute(query, conn);
        } 
    }

    public int execute(Query query, Connection conn) throws SQLException, QueryBuilderException {

        int c;
        ParametrizedQuery pq = getDialect().dryRun(query, false, conn);
        String queryString = pq.getQuery();
        try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

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

    public List<JoinNode> toJoinNodes(TableType tableType, List<Join> joins, Connection conn) throws QueryBuilderException, SQLException {

        List<JoinNode> rootJoinNodes = new ArrayList();
        for (Join j : joins) {

            String[] path = j.getFieldPath().split("[.]");
            if (path.length <= 0) {
                throw new QueryBuilderException("No se encuentra el path " + j.getFieldPath());
            }

            joins(path, j.getAlias(), rootJoinNodes, tableType, j.getJointype(), conn);
        }
        return rootJoinNodes;
    }

    private void joins(String[] path, String alias, List<JoinNode> joins, TableType tableType, Query.JOINTYPE joinType, Connection conn) throws QueryBuilderException, SQLException {

        String entityFieldName = path[0];

        String nodeAlias;
        Query.JOINTYPE nodeJointype;

        if (path.length == 1) {
            nodeAlias = alias;
            nodeJointype = joinType;
        } else {
            nodeAlias = "_" + entityFieldName;
            nodeJointype = Query.JOINTYPE.INNER;
        }

        java.util.function.Predicate<JoinNode> p = n -> Objects.equals(n.getFieldName(), entityFieldName);
        JoinNode node = joins.stream().filter(p).findAny().orElse(null);
        if (node == null) {
            node = getJoinNode(tableType, entityFieldName, nodeAlias, conn);
            node.setJoinType(nodeJointype);
            joins.add(node);
        }

        if (path.length <= 1) {
            return;
        }

        String[] sub = Arrays.copyOfRange(path, 1, path.length);
        joins(sub, alias, node.getChildren(), node.getChildTableType(), joinType, conn);
    }


    private JoinNode getJoinNode(TableType tableType, String fieldName, String alias, Connection conn) throws QueryBuilderException, SQLException {

        ManyToOneType m2ot = tableType.getManyToOneType(fieldName);
        TableType t = getTableType(m2ot.getFieldClass(), conn);

        List<ColumnType> selects = t.getColumns();
        List<PowerColumnType> selectColumns = selects.stream().map(e -> {
            PowerColumnType sct = new PowerColumnType(e);
            sct.setColumnAlias(String.format("%s_%s", alias, e.getColumnName()));
            return sct;
        }).collect(Collectors.toList());

        JoinNode node = new JoinNode(alias, m2ot);
        node.setChildTableType(t);
        node.setSelectColumns(selectColumns);

        return node;
    }

    /*public PowerColumnType getAliasColumnType(String fieldPath, Query query, List<JoinNode> rootJoinNodes) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return getAliasColumnType(fieldPath, query, rootJoinNodes, conn);
        }
    }*/

    public PowerColumnType getAliasColumnType(String fieldPath, Query query, List<JoinNode> rootJoinNodes, Connection conn) throws SQLException, QueryBuilderException {

        TableType tableType = getTableType(query.getEntityClass(), conn);

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

        if (alias.equals(query.getRootAlias())) {
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

    public String getMetadaSchema() {
        return metadaSchema;
    }

    public void setMetadaSchema(String metadaSchema) {
        this.metadaSchema = metadaSchema;
    }

}
