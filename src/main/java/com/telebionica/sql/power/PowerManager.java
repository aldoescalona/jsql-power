/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.power;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.dialect.Dialect;
import com.telebionica.sql.join.Join;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.predicates.Predicate;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.setu.SetForUpdate;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.GeneratorType;
import com.telebionica.sql.type.ManyToManyType;
import com.telebionica.sql.type.ManyToOneType;
import com.telebionica.sql.type.TableType;
import com.telebionica.sql.util.Generator;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

/**
 *
 * @author aldo
 *
 */
public abstract class PowerManager {

    private String metadaSchema;
    private final Map<Class, TableType> tableTypeMap = new HashMap<>();
    private final Map<String, Generator> generatorMap = new HashMap<>();
    private Dialect dialect = null;

    public abstract Connection getConnection() throws SQLException;

    public <E> Query createQuery() throws SQLException, QueryBuilderException {
        Query<E> query = new Query(this);
        return query;
    }

    public int insert(Object e) throws SQLException, QueryBuilderException {
        return insert(null, e);
    }

    public int insert(String schema, Object e) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return insert(schema, e, conn);
        }
    }

    public int insert(String schema, Object e, Connection conn) throws SQLException, QueryBuilderException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

        List<PowerColumnType> insertParams = new ArrayList();
        List<PowerColumnType> autos = new ArrayList();

        List<ColumnType> cols = tableType.getColumns();
        for (ColumnType ct : cols) {

            if (!ct.isPrimary() || ct.getGeneratorType() == null) {
                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(ct.getColumnName());
                param.getter(e);
                insertParams.add(param);
            } else {
                GeneratorType gt = ct.getGeneratorType();
                if (gt.getStrategy() == GenerationType.AUTO) {
                    PowerColumnType auto = new PowerColumnType(ct);
                    autos.add(auto);
                } else if (gt.getStrategy() == GenerationType.SEQUENCE) {
                    String seqQueryString = dialect.nextVal(schema, gt.getGenerator());
                    try (PreparedStatement pstm = conn.prepareStatement(seqQueryString); ResultSet rs = pstm.executeQuery()) {
                        if (rs.next()) {
                            PowerColumnType secpow = new PowerColumnType(ct);
                            secpow.setColumnAlias(ct.getColumnName());
                            secpow.push(e, rs, 1);
                            secpow.getter(e);
                            insertParams.add(secpow);
                        }
                    }
                } else if (gt.getStrategy() == GenerationType.IDENTITY) {

                    Generator gen = getGenerator(gt);
                    Serializable id = gen.next(schema, gt.getName());

                    PowerColumnType secpow = new PowerColumnType(ct);
                    secpow.setColumnAlias(ct.getColumnName());
                    secpow.setter(e, id);
                    secpow.getter(e);
                    insertParams.add(secpow);
                }
            }
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

        PreparedStatement pstm = null;
        try {
            if (autos.isEmpty()) {
                pstm = conn.prepareStatement(query);
            } else {
                pstm = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            }

            int i = 1;
            for (PowerColumnType powerValue : insertParams) {
                powerValue.powerStatement(pstm, i++);
            }
            pstm.executeUpdate();

            if (!autos.isEmpty()) {
                try (ResultSet rs = pstm.getGeneratedKeys()) {
                    int index = 1;
                    for (PowerColumnType auto : autos) {
                        if (rs.next()) {
                            auto.push(e, rs, index++);
                        }
                    }
                }
            }
        } finally {
            if (pstm != null) {
                pstm.close();
            }
        }

        System.out.println(" INSERT " + query);

        return 0;
    }

    private Generator getGenerator(GeneratorType gt) throws QueryBuilderException {

        Generator gen = generatorMap.get(gt.getName());
        if (gen == null) {
            try {
                Class<Generator> gclass = (Class<Generator>) Class.forName(gt.getGenerator());
                gen = gclass.getConstructor().newInstance();
                generatorMap.put(gt.getName(), gen);
            } catch (Exception ex) {
                Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                throw new QueryBuilderException("Generador " + gt.getGenerator() + " invalido", ex);
            }
        }

        return gen;
    }

    public int update(Object e, String... fields) throws SQLException, QueryBuilderException {
        return update(null, e, fields);
    }

    public int update(String schema, Object e, String... fields) throws SQLException, QueryBuilderException {
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

    public int delete(Object e) throws SQLException, QueryBuilderException {
        return delete(null, e);
    }

    public int delete(String schema, Object e) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return delete(schema, e, conn);
        }
    }

    public int delete(String schema, Object e, Connection conn) throws SQLException, QueryBuilderException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

        List<ColumnType> ids = tableType.getColumns();
        ids = ids.stream().filter(c -> c.isPrimary()).collect(Collectors.toList());

        List<PowerColumnType> whereParams = new ArrayList();
        for (ColumnType ct : ids) {
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(ct.getColumnName());
            param.getter(e);
            whereParams.add(param);
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder sbvalues = new StringBuilder();
        sb.append("DELETE FROM ");

        if (schema != null) {
            sb.append(schema).append(".");
        }

        sb.append(tableType.getName());

        List<PowerColumnType> orderParams = new ArrayList();

        Iterator<PowerColumnType> colIt = whereParams.iterator();
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

        System.out.println(" DELETE QUERY: " + query);
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
        List<ManyToManyType> manyToManyTypes = new ArrayList();

        Field[] scopeFields = entityClass.getDeclaredFields();

        for (Field field : scopeFields) {
            addColumnType(field, columns, tableType);
            addManyToOneType(field, manyToOneTypes, tableType);
            addManyToManyType(field, manyToManyTypes, tableType);
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
        sb.append(" WHERE 1 = 0");

        String query = sb.toString();
        query = getDialect().limit(query, 1);

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

        Gson gson = new GsonBuilder()
                .excludeFieldsWithModifiers(Modifier.PROTECTED)
                .setPrettyPrinting().create();
        String json = gson.toJson(tableType);

        System.out.println("TablaType " + json);

        return tableType;
    }

    private void addColumnType(Field field, List<ColumnType> columns, TableType tableType) {
        Column colann = field.getAnnotation(Column.class);
        if (colann != null) {
            ColumnType ct = new ColumnType(colann.name(), field.getName(), field.getType(), tableType);

            Id idann = field.getAnnotation(Id.class);
            if (idann != null) {
                ct.setPrimary(true);
                GeneratedValue gen = field.getAnnotation(GeneratedValue.class);
                if (gen != null) {
                    String name = String.format("%s_%s", tableType.getEntityClass().getSimpleName(), field.getName());
                    GeneratorType gt = new GeneratorType(gen.strategy(), name, gen.generator());
                    ct.setGeneratorType(gt);
                }
            } else {
                ct.setPrimary(false);
            }

            Enumerated enu = field.getAnnotation(Enumerated.class);
            ct.setEnumerated(enu);

            columns.add(ct);
        }
    }

    private void addManyToManyType(Field field, List<ManyToManyType> manyToManyTypes, TableType tableType) throws QueryBuilderException {

        ManyToMany m2m = field.getAnnotation(ManyToMany.class);
        if (m2m != null) {
            try {

                Class collectionRelatedClass = getCollectionRelatedClass(field);
                if (m2m.mappedBy().isEmpty()) {
                    JoinTable jt = field.getAnnotation(JoinTable.class);
                    if (jt != null) {
                        ManyToManyType m2mt = new ManyToManyType(field.getName(), field.getType(), collectionRelatedClass, jt, tableType);
                        manyToManyTypes.add(m2mt);
                    }
                } else {
                    String mbyFieldName = m2m.mappedBy();
                    Field relatedField = collectionRelatedClass.getDeclaredField(mbyFieldName);
                    
                    JoinTable jt = relatedField.getAnnotation(JoinTable.class);
                    if (jt != null) {
                        ManyToManyType m2mt = new ManyToManyType(field.getName(), field.getType(), collectionRelatedClass, jt, tableType, true);
                        manyToManyTypes.add(m2mt);
                    }
                }
            } catch (QueryBuilderException | NoSuchFieldException | SecurityException e) {
                e.printStackTrace();
                throw new QueryBuilderException("No se logro obtener reverso manyToMany del atributo " + field.getName(), e);
            }

        }
    }

    private Class getCollectionRelatedClass(Field field) throws QueryBuilderException {

        Class collectionRelatedClass = null;
        Class returnClass = field.getType();
        if (Collection.class.isAssignableFrom(returnClass)) {

            Type returnType = field.getGenericType();
            if (returnType instanceof ParameterizedType) {
                try {
                    ParameterizedType paramType = (ParameterizedType) returnType;
                    Type[] argTypes = paramType.getActualTypeArguments();
                    if (argTypes.length > 0) {
                        collectionRelatedClass = ((Class) argTypes[0]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new QueryBuilderException("No se logro obtener la clase relacionada, utilice notacion generica <Ent>" + field.getName(), e);
                }
            }
        }

        if (collectionRelatedClass == null) {
            throw new QueryBuilderException("No se logro obtener la clase relacionada, utilice notacion generica <Ent>" + field.getName());
        }
        return collectionRelatedClass;
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

    public Dialect getDialect() throws QueryBuilderException {
        if (dialect == null && this.getClass().isAnnotationPresent(Power.class)) {
            try {
                Power ann = this.getClass().getAnnotation(Power.class);
                Class<? extends Dialect> theclass = ann.dialect();
                dialect = theclass.getConstructor(PowerManager.class).newInstance(this);
            } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
                throw new QueryBuilderException("No se ha definido un dialect valido ", e);
            }
        }

        return dialect;
    }

    public <E> List<E> list(Query<E> query) throws QueryBuilderException, SQLException {
        try (Connection conn = getConnection()) {
            return list(query, conn);
        }
    }

    public <E> List<E> list(Query<E> query, Connection conn) throws QueryBuilderException, SQLException {
        List<E> list = new ArrayList();

        ParametrizedQuery pq = dryRun(query, conn);
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
                        Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
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

    public List list(Object e, String listFieldName, Order... orders) throws QueryBuilderException, SQLException {
        try (Connection conn = getConnection()) {
            return list(e, listFieldName, conn, orders);
        }
    }

    public List list(Object e, String listFieldName, Connection conn, Order... orders) throws QueryBuilderException, SQLException {

        return new ArrayList();
    }

    public Integer count(Query query) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
            return count(query, conn);
        }
    }

    public Integer count(Query query, Connection conn) throws SQLException, QueryBuilderException {

        int c = 0;

        ParametrizedQuery pq = dryRun(query, true, conn);
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
        try (Connection conn = getConnection()) {
            return execute(query, conn);
        }
    }

    public int execute(Query query, Connection conn) throws SQLException, QueryBuilderException {

        int c;
        ParametrizedQuery pq = dryRun(query, conn);
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

    public ParametrizedQuery dryRun(Query query, boolean count, Connection conn) throws SQLException, QueryBuilderException {

        ParametrizedQuery pq = null;
        if (query.getQtype() == Query.QTYPE.SELECT) {
            pq = selectParametrizedQuery(query, count, conn);
        } else if (query.getQtype() == Query.QTYPE.DELETE) {
            pq = deleteParametrizedQuery(query, conn);
        } else if (query.getQtype() == Query.QTYPE.UPDATE) {
            pq = updateParametrizedQuery(query, conn);
        }

        System.out.println(" QUERY: " + pq.getQuery());

        return pq;
    }

    public ParametrizedQuery dryRun(Query query, Connection conn) throws SQLException, QueryBuilderException {
        return dryRun(query, false, conn);
    }

    public ParametrizedQuery dryRun(Query query) throws SQLException, QueryBuilderException {
        return dryRun(query, false);
    }

    public ParametrizedQuery dryRun(Query query, boolean count) throws SQLException, QueryBuilderException {
        try (Connection conn = getConnection()) {
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

    protected List<PowerColumnType> selectAliasIndexator(Query query, TableType tableType) {

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

    private ParametrizedQuery selectParametrizedQuery(Query query, boolean count, Connection conn) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        List<JoinNode> rootJoinNodes = toJoinNodes(tableType, query.getJoins(), conn);

        String queryString = "";

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query);

        sb.append("SELECT ");
        if (!count) {

            selectColumns = selectAliasIndexator(query, tableType);

            Iterator<PowerColumnType> selectIt = selectColumns.iterator();
            while (selectIt.hasNext()) {
                PowerColumnType selColumnType = selectIt.next();

                sb.append(query.getRootAlias()).append(".");
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
        if (query.getSchema() != null) {
            sb.append(query.getSchema()).append(".");
        }
        sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");
        sb.append(joiners);

        StringBuilder wheresb = new StringBuilder();
        if (query.getPredicates().size() > 0) {
            wheresb.append("\nWHERE ");

            Iterator<Predicate> it = query.getPredicates().iterator();
            while (it.hasNext()) {
                Predicate p = it.next();
                p.build(rootJoinNodes, conn);

                wheresb.append(p.getPredicateStatement());
                if (p.hasValues()) {
                    params.addAll(p.getValueTypes());
                }

                if (it.hasNext()) {
                    wheresb.append(" AND ");
                }
            }
        }
        sb.append(wheresb);

        if (count) {
            queryString = sb.toString();
        } else {
            if (query.getOrders().size() > 0) {
                sb.append("\nORDER BY ");
                Iterator<Order> it = query.getOrders().iterator();
                while (it.hasNext()) {
                    Order order = it.next();
                    order.build(rootJoinNodes, conn);
                    sb.append(order.getOrderStatement());

                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            queryString = sb.toString();
            queryString = getDialect().limit(query.getSchema(), queryString, query.getFirstResult(), query.getMaxResults());
        }

        ParametrizedQuery pq = new ParametrizedQuery(queryString, params, selectColumns, rootJoinNodes);
        return pq;

    }

    private ParametrizedQuery deleteParametrizedQuery(Query query, Connection conn) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        List<JoinNode> rootJoinNodes = toJoinNodes(tableType, query.getJoins(), conn);

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query);

        sb.append("DELETE ");

        List<String> deletes = new ArrayList();
        if (query.getDeleteAliases() == null || query.getDeleteAliases().length == 0) {
            deletes.add(query.getRootAlias());
        } else {
            deletes.addAll(Arrays.asList(query.getDeleteAliases()));
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
        if (query.getSchema() != null) {
            sb.append(query.getSchema()).append(".");
        }
        sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");
        sb.append(joiners);

        StringBuilder wheresb = new StringBuilder();
        if (query.getPredicates().size() > 0) {
            wheresb.append("\nWHERE ");

            Iterator<Predicate> it = query.getPredicates().iterator();
            while (it.hasNext()) {
                Predicate p = it.next();
                p.build(rootJoinNodes, conn);

                wheresb.append(p.getPredicateStatement());
                params.addAll(p.getValueTypes());

                if (it.hasNext()) {
                    wheresb.append(" AND ");
                }
            }
        }
        sb.append(wheresb);

        String queryString = sb.toString();

        ParametrizedQuery pq = new ParametrizedQuery(queryString, params, rootJoinNodes);
        return pq;
    }

    private ParametrizedQuery updateParametrizedQuery(Query query, Connection conn) throws SQLException, QueryBuilderException {
        List<PowerColumnType> params = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        List<JoinNode> rootJoinNodes = toJoinNodes(tableType, query.getJoins(), conn);

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query);
        sb.append("UPDATE ");
        if (query.getSchema() != null) {
            sb.append(query.getSchema()).append(".");
        }
        sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");

        sb.append(joiners);

        if (!query.getSets().isEmpty()) {
            sb.append("\nSET ");
        }

        Iterator<SetForUpdate> it = query.getSets().iterator();
        while (it.hasNext()) {
            SetForUpdate p = it.next();
            p.build(rootJoinNodes, conn);

            sb.append(p.getAsignStatement());
            if (p.hasValue()) {
                params.add(p.getValueType());
            }

            if (it.hasNext()) {
                sb.append(", ");
            }
        }

        StringBuilder wheresb = new StringBuilder();
        if (query.getPredicates().size() > 0) {
            wheresb.append("\nWHERE ");

            Iterator<Predicate> itp = query.getPredicates().iterator();
            while (itp.hasNext()) {
                Predicate p = itp.next();
                p.build(rootJoinNodes, conn);

                wheresb.append(p.getPredicateStatement());
                params.addAll(p.getValueTypes());

                if (itp.hasNext()) {
                    wheresb.append(" AND ");
                }
            }
        }
        sb.append(wheresb.toString());
        String queryString = sb.toString();
        ParametrizedQuery pq = new ParametrizedQuery(queryString, params, rootJoinNodes);
        return pq;
    }

    public String getMetadaSchema() {
        return metadaSchema;
    }

    public void setMetadaSchema(String metadaSchema) {
        this.metadaSchema = metadaSchema;
    }
}