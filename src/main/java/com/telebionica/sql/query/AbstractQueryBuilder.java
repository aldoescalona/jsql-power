/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.SelectColumnType;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.TableType;
import java.lang.reflect.Field;
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
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.persistence.JoinColumns;

/**
 *
 * @author aldo
 */
public abstract class AbstractQueryBuilder {

    private String metadaSchema;

    public abstract Connection getConnection() throws SQLException;

    private Map<Class, TableType> tableTypeMap = new HashMap<>();

    public <E> Query createQuery() throws SQLException, QueryBuilderException {
        Query<E> query = new Query(this);
        return query;
    }

    private <E> TableType buildTableType(Class<E> entityClass) throws SQLException, QueryBuilderException {

        Table ann = (Table) entityClass.getDeclaredAnnotation(Table.class);
        if (ann == null) {
            throw new QueryBuilderException("No existe la anotacion @Table en la clase " + entityClass);
        }

        String tableNeme = ann.name();

        List<ColumnType> colums = new ArrayList();

        Field[] scopeFields = entityClass.getDeclaredFields();

        for (Field f : scopeFields) {
            Column colann = f.getAnnotation(Column.class);
            if (colann != null) {

                ColumnType ct = new ColumnType(colann.name(), f.getName(), f.getType());
                Id idann = f.getAnnotation(Id.class);
                ct.setPrimary(idann != null);
                colums.add(ct);

            }
        }

        StringBuilder sb = new StringBuilder("SELECT ");
        Iterator<ColumnType> colIt = colums.iterator();

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

        try ( Connection conn = getConnection();  PreparedStatement pst = conn.prepareStatement(query);  ResultSet rs = pst.executeQuery()) {
            ResultSetMetaData mdrd = rs.getMetaData();

            for (int i = 0; i < colums.size(); i++) {
                int index = i + 1;

                ColumnType columnType = colums.get(i);

                String cname = mdrd.getColumnName(index);
                Integer ctype = mdrd.getColumnType(index);

                columnType.setColumnName(cname);
                columnType.setType(ctype);

                if (ctype == Types.DECIMAL) {
                    columnType.setScale(mdrd.getScale(index));
                }
            }
        }

        TableType tableType = new TableType(tableNeme, entityClass);

        tableType.setColumns(colums);

        Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        String json = gson.toJson(tableType);

        System.out.println("TablaType " + json);

        return tableType;
    }

    public String getMetadaSchema() {
        return metadaSchema;
    }

    public void setMetadaSchema(String metadaSchema) {
        this.metadaSchema = metadaSchema;
    }

    public TableType getTableType(Class entityClass) throws SQLException, QueryBuilderException {

        TableType tableType = tableTypeMap.get(entityClass);
        if (tableType == null) {
            tableType = buildTableType(entityClass);
            tableTypeMap.put(entityClass, tableType);
        }
        return tableType;
    }

    public JoinNode getJoinNode(Class entityClass, String fieldName, String alias) throws QueryBuilderException, SQLException {

        Field field = null;
        try {
            field = entityClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException | SecurityException e) {
            Logger.getGlobal().log(Level.SEVERE, e.getMessage(), e);
        }

        if (field == null) {
            throw new QueryBuilderException("No se encuentra el campo " + fieldName + " de la clase " + entityClass);
        }

        /*ManyToOne m2o = field.getAnnotation(ManyToOne.class);
        if(m2o == null){
            throw new QueryBuilderException(" JOIN soportado solo para ManyToOne ");
        }*/
        
        JoinColumn jcann = field.getAnnotation(JoinColumn.class);
        JoinColumns jcanns = field.getAnnotation(JoinColumns.class);

        List<JoinColumn> list = null;
        if (jcann != null) {
            list = new ArrayList();
            list.add(jcann);
        }

        if (jcanns != null) {
            list = Arrays.asList(jcanns.value());
        }

        if (list == null || list.isEmpty()) {
            throw new QueryBuilderException("No se encuentra la anotacion @JoinColumn o @JoinColumns en el campo " + fieldName + " de la clase " + entityClass);
        }

        Class fieldClass = field.getType();
        TableType tt = getTableType(fieldClass);

        
        List<ColumnType> selects = tt.getColumns();
        List<SelectColumnType> selectColumns = selects.stream().map(e -> {
            SelectColumnType sct = new SelectColumnType(String.format("%s_%s", alias, e.getColumnName()), e);
            return sct;
        }).collect(Collectors.toList());

        JoinNode node = new JoinNode();
        node.setJoiners(list);
        node.setFieldName(fieldName);
        node.setTableType(tt);
        node.setAlias(alias);
        node.setSelectColumns(selectColumns);

        return node;

    }

    public void joins(String[] path, String alias, List<JoinNode> joins, Class theClass, Query.JOINTYPE joinType) throws QueryBuilderException, SQLException {

        String root = path[0];

        String nodeAlias;
        Query.JOINTYPE nodeJointype;

        if (path.length == 1) {
            nodeAlias = alias;
            nodeJointype = joinType;
        } else {
            nodeAlias = "_" + root;
            nodeJointype = Query.JOINTYPE.INNER;
        }

        Predicate<JoinNode> p = n -> Objects.equals(n.getFieldName(), root);
        JoinNode node = joins.stream().filter(p).findAny().orElse(null);
        if (node == null) {
            node = getJoinNode(theClass, root, nodeAlias);
            // node.setAlias(nodeAlias);
            node.setJoinType(nodeJointype);
            joins.add(node);
        }

        if (path.length <= 1) {
            return;
        }

        String[] sub = Arrays.copyOfRange(path, 1, path.length);
        joins(sub, alias, node.getChildren(), node.getTableType().getEntityClass(), joinType);
    }
}
