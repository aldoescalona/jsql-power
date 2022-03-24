/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.power;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
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
    private Map<Class, TableType> tableTypeMap = new HashMap<>();
    
    public abstract Connection getConnection() throws SQLException;
    
    public int insert(Object e) throws SQLException, QueryBuilderException {
        return insert(null, e);
    }
    
    public int insert(String schema, Object e) throws SQLException, QueryBuilderException {

        Class entityClass =  e.getClass();
        TableType tableType = this.getTableType(entityClass);

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

            TableType otherTT = this.getTableType(m2o.getFieldClass());

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

        try ( Connection conn = this.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

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
    
    public int update(String schema, Object e, String... fields) throws SQLException, QueryBuilderException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass);

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

            TableType otherTT = this.getTableType(m2o.getFieldClass());

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
        try ( Connection conn = this.getConnection();  PreparedStatement pstm = conn.prepareStatement(query)) {

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
    
    public TableType getTableType(Class entityClass) throws SQLException, QueryBuilderException {

        TableType tableType = tableTypeMap.get(entityClass);
        if (tableType == null) {
            tableType = buildTableType(entityClass);
            tableTypeMap.put(entityClass, tableType);
        }
        return tableType;
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
    
    public <E> Query createQuery() throws SQLException, QueryBuilderException {
        Query<E> query = new Query(this);
        return query;
    }
    
    public String getMetadaSchema() {
        return metadaSchema;
    }

    public void setMetadaSchema(String metadaSchema) {
        this.metadaSchema = metadaSchema;
    }
    
}
