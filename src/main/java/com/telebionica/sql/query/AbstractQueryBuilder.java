/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.PowerColumnType;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.ManyToOneType;
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

    private void addColumnType(Field field, List<ColumnType> columns, TableType tableType ) {
        Column colann = field.getAnnotation(Column.class);
        if (colann != null) {
            ColumnType ct = new ColumnType(colann.name(), field.getName(), field.getType(), tableType);
            Id idann = field.getAnnotation(Id.class);
            ct.setPrimary(idann != null);
            columns.add(ct);
        }
    }

    private void addManyToOneType(Field field, List<ManyToOneType> manyToOneTypes, TableType tableType ) {
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
        
        if(joiners != null && !joiners.isEmpty()){
            ManyToOneType m2ot = new ManyToOneType(field.getName(), field.getType(), joiners, tableType);
            manyToOneTypes.add(m2ot);
        }
    }

    private <E> TableType buildTableType(Class<E> entityClass) throws SQLException, QueryBuilderException {

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

        try ( Connection conn = getConnection();  PreparedStatement pst = conn.prepareStatement(query);  ResultSet rs = pst.executeQuery()) {
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

    public JoinNode getJoinNode(TableType tableType, String fieldName, String alias) throws QueryBuilderException, SQLException {

        ManyToOneType m2ot = tableType.getManyToOneType(fieldName);
        TableType t = getTableType(m2ot.getFieldClass());
        
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

    public void joins(String[] path, String alias, List<JoinNode> joins, TableType tableType, Query.JOINTYPE joinType) throws QueryBuilderException, SQLException {

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

        Predicate<JoinNode> p = n -> Objects.equals(n.getFieldName(), entityFieldName);
        JoinNode node = joins.stream().filter(p).findAny().orElse(null);
        if (node == null) {
            node = getJoinNode(tableType, entityFieldName, nodeAlias);
            node.setJoinType(nodeJointype);
            joins.add(node);
        }

        if (path.length <= 1) {
            return;
        }

        String[] sub = Arrays.copyOfRange(path, 1, path.length);
        joins(sub, alias, node.getChildren(), node.getChildTableType(), joinType);
    }
}
