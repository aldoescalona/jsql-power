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
import com.telebionica.sql.query.CollectionParametrizedQuery;
import com.telebionica.sql.query.Fetch;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.query.RootParametrizedQuery;
import com.telebionica.sql.setu.SetForUpdate;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.GeneratorType;
import com.telebionica.sql.type.ManyToManyType;
import com.telebionica.sql.type.JoinColumnsType;
import com.telebionica.sql.type.TableType;
import com.telebionica.sql.util.Generator;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
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

        List<JoinColumnsType> jcstList = tableType.getJoinColumns();
        for (JoinColumnsType jct : jcstList) {

            Object obj = jct.getter(e);
            if (obj == null) {
                continue;
            }

            TableType otherTT = this.getTableType(jct.getFieldClass(), conn);

            List<JoinColumn> jcs = jct.getJoiners();
            for (JoinColumn jc : jcs) {
                if (jc.insertable()) {

                    boolean reverse = jct.isReverse();
                    String jcName = reverse ? jc.referencedColumnName() : jc.name();
                    String referencedColumnName = reverse ? jc.name() : jc.referencedColumnName();

                    ColumnType ct = otherTT.getColumnType(referencedColumnName);
                    PowerColumnType param = new PowerColumnType(ct);
                    param.setColumnAlias(jcName);
                    param.getter(obj);
                    insertParams.add(param);
                }

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

        List<JoinColumnsType> jctList = tableType.getJoinColumns();
        for (JoinColumnsType jct : jctList) {

            Object obj = jct.getter(e);
            if (obj == null) {
                continue;
            }

            TableType otherTT = this.getTableType(jct.getFieldClass(), conn);

            List<JoinColumn> jcs = jct.getJoiners();
            for (JoinColumn jc : jcs) {
                if (jc.updatable()) {

                    boolean reverse = jct.isReverse();
                    String jcName = reverse ? jc.referencedColumnName() : jc.name();
                    String referencedColumnName = reverse ? jc.name() : jc.referencedColumnName();

                    ColumnType ct = otherTT.getColumnType(referencedColumnName);
                    PowerColumnType param = new PowerColumnType(ct);
                    param.setColumnAlias(jcName);
                    param.getter(obj);
                    updateParams.add(param);
                }
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

        String queryString = sb.toString();

        System.out.println(" DELETE QUERY: " + queryString);
        int c = 0;
        try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

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
        List<JoinColumnsType> joinColumnsTypes = new ArrayList();
        List<ManyToManyType> manyToManyTypes = new ArrayList();

        Field[] scopeFields = entityClass.getDeclaredFields();

        for (Field field : scopeFields) {
            addColumnType(field, columns, tableType);
            addJoinColumnsType(field, joinColumnsTypes, tableType);
            addManyToManyType(field, manyToManyTypes, tableType);
            // addOneToManyType();
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
        tableType.setJoinColumns(joinColumnsTypes);
        tableType.setManyToManyTypes(manyToManyTypes);

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

    private List<JoinColumn> getJoiners(Field field) {
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
        return joiners;
    }

    private void addJoinColumnsType(Field field, List<JoinColumnsType> joinColumnsTypes, TableType tableType) throws QueryBuilderException {

        ManyToOne mto = field.getAnnotation(ManyToOne.class);
        OneToOne oto = field.getAnnotation(OneToOne.class);

        if (mto == null && oto == null) {
            return;
        }

        if (mto != null) {
            List<JoinColumn> joiners = getJoiners(field);
            JoinColumnsType cjt = new JoinColumnsType(field.getName(), field.getType(), joiners, tableType);
            joinColumnsTypes.add(cjt);
            return;
        }

        if (oto != null) {
            boolean reverse = oto.mappedBy() != null && !oto.mappedBy().isEmpty();
            if (reverse) {
                try {
                    Class raletedClass = field.getType();
                    Field relatedField = raletedClass.getDeclaredField(oto.mappedBy());
                    OneToOne relatedOto = relatedField.getAnnotation(OneToOne.class);
                    if (relatedOto != null) {
                        List<JoinColumn> joiners = getJoiners(relatedField);
                        JoinColumnsType cjt = new JoinColumnsType(field.getName(), field.getType(), joiners, tableType, reverse);
                        joinColumnsTypes.add(cjt);
                    }
                } catch (NoSuchFieldException | SecurityException e) {
                    throw new QueryBuilderException("No se logro obtener reverso manyToMany del atributo " + field.getName(), e);
                }
            } else {
                List<JoinColumn> joiners = getJoiners(field);
                JoinColumnsType cjt = new JoinColumnsType(field.getName(), field.getType(), joiners, tableType);
                joinColumnsTypes.add(cjt);
            }

        }

        /*List<JoinColumn> joiners = getJoiners(field);
        if (joiners != null && !joiners.isEmpty()) {

            ManyToOne mto = field.getAnnotation(ManyToOne.class);
            OneToOne oto = field.getAnnotation(OneToOne.class);
            
            boolean reverse = oto != null && oto.mappedBy() != null && !oto.mappedBy().isEmpty();

            if (mto != null || !reverse) {
                JoinColumnsType cjt = new JoinColumnsType(field.getName(), field.getType(), joiners, tableType);
                joinColumnsTypes.add(cjt);
            } else {

                
            }

        }*/
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

        RootParametrizedQuery parametrizedQuery = (RootParametrizedQuery) dryRun(query, conn);
        String queryString = parametrizedQuery.getQuery();

        try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : parametrizedQuery.getParams()) {
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
                    for (PowerColumnType st : parametrizedQuery.getSelectColumns()) {
                        st.push(rootInstance, rs);
                    }
                    push(rootInstance, parametrizedQuery.getJoinNodes(), rs);

                    List<Fetch> fetchs = query.getFetchs();
                    for (Fetch fetch : fetchs) {
                        CollectionParametrizedQuery pq = parametrizedQuery.getFetchParametrizedQuery(fetch.getCollectionField());
                        if (pq != null) {

                            List fetchList = getCollection(rootInstance, pq, conn);
                            push(query.getEntityClass(), fetch.getCollectionField(), rootInstance, fetchList);

                            /*Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
                            String json = gson.toJson(rootInstance);
                            System.out.println(" LIST: " + json);*/ 
                        }
                    }

                    list.add(rootInstance);
                }
            }
        }

        // Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        // String json = gson.toJson(list);
        // System.out.println(" LIST: " + json);
        return list;
    }

    private List getCollection(Object rootInstance, CollectionParametrizedQuery pq, Connection conn) throws QueryBuilderException, SQLException {
        List fetchList = new ArrayList();
        try (PreparedStatement pstm = conn.prepareStatement(pq.getQuery())) {

            int j = 1;
            for (PowerColumnType pct : pq.getParams()) {
                pct.getter(rootInstance);
                pct.powerStatement(pstm, j++);
            }

            try (ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    Object instance;
                    try {
                        instance = pq.getTargetClass().getConstructor().newInstance();
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                        Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                        throw new QueryBuilderException(" No se pudo crear instancia de la clase query.getEntityClass() ", ex);
                    }
                    for (PowerColumnType st : pq.getSelectColumns()) {
                        st.push(instance, rs);
                    }
                    push(instance, pq.getJoinNodes(), rs);
                    fetchList.add(instance);
                }
            }

        }
        return fetchList;
    }

    public void push(Class targetClass, String fieldName, Object target, List collection) throws QueryBuilderException {
        try {
            getWriteMethod(targetClass, fieldName).invoke(target, collection);
        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new QueryBuilderException(e);
        }
    }

    public Method getWriteMethod(Class targetClass, String fieldName) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(targetClass);
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(fieldName)) {
                return pd.getWriteMethod();
            }
        }
        return null;
    }

    public Method getReadMethod(Class targetClass, String fieldName) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(targetClass);
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (pd.getName().equalsIgnoreCase(fieldName)) {
                return pd.getReadMethod();
            }
        }
        return null;
    }


    /*public List list(Object e, String listFieldName, String schema, Order... orders) throws QueryBuilderException, SQLException {
        try (Connection conn = getConnection()) {
            return list(e, listFieldName, schema, conn, orders);
        }
    }

    public List list(Object e, String listFieldName, String schema, Connection conn, Order... orders) throws QueryBuilderException, SQLException {

        List list = new ArrayList();
        TableType tableType = getTableType(e.getClass());
        ManyToManyType m2mt = tableType.getManyToManyType(listFieldName);

        if (m2mt != null) {
            Class relatedClass = m2mt.getCollectionRelatedClass();
            TableType relatedTableType = getTableType(relatedClass);

            List<PowerColumnType> select = selectAliasIndexator(relatedTableType, "e");

            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");

            Iterator<PowerColumnType> selectIt = select.iterator();
            while (selectIt.hasNext()) {
                PowerColumnType selColumnType = selectIt.next();

                sb.append("e").append(".");
                sb.append(selColumnType.getColumnType().getColumnName());
                sb.append(" as ").append(selColumnType.getColumnAlias());

                if (selectIt.hasNext()) {
                    sb.append(", ");
                }
            }

            sb.append("\nFROM ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(relatedTableType.getName()).append(" ").append("e").append(" ");

            System.out.println(" " + sb.toString());

            JoinTable jt = m2mt.getJoinTable();

            sb.append("\nINNER JOIN ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(jt.name()).append(" tr ON ");

            JoinColumn[] jcs = m2mt.isReverse() ? jt.joinColumns() : jt.inverseJoinColumns();
            Iterator<JoinColumn> jcsit = Arrays.asList(jcs).iterator();
            while (jcsit.hasNext()) {
                JoinColumn jc = jcsit.next();
                sb.append("tr.").append(jc.name()).append(" = ").append("e.").append(jc.referencedColumnName());
                if (jcsit.hasNext()) {
                    sb.append(" AND ");
                }
            }

            sb.append("\nINNER JOIN ");
            if (schema != null) {
                sb.append(schema).append(".");
            }
            sb.append(tableType.getName()).append(" _e ON ");

            JoinColumn[] ijcs = m2mt.isReverse() ? jt.inverseJoinColumns() : jt.joinColumns();
            Iterator<JoinColumn> ijcsit = Arrays.asList(ijcs).iterator();
            while (ijcsit.hasNext()) {
                JoinColumn jc = ijcsit.next();
                sb.append("tr.").append(jc.name()).append(" = ").append("_e.").append(jc.referencedColumnName());
                if (ijcsit.hasNext()) {
                    sb.append(" AND ");
                }
            }

            List<ColumnType> ids = tableType.getColumns();
            ids = ids.stream().filter(c -> c.isPrimary()).collect(Collectors.toList());

            List<PowerColumnType> whereParams = new ArrayList();
            for (ColumnType ct : ids) {
                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(ct.getColumnName());
                param.getter(e);
                whereParams.add(param);
            }

            sb.append("\nWHERE ");
            Iterator<PowerColumnType> colIt = whereParams.iterator();
            while (colIt.hasNext()) {
                PowerColumnType ct = colIt.next();
                if (ct.getColumnType().isPrimary()) {
                    sb.append("_e.").append(ct.getColumnAlias());
                    sb.append(" = ?");
                    //orderParams.add(ct);
                    if (colIt.hasNext()) {
                        sb.append(" AND ");
                    }
                }
            }

            if (orders.length > 0) {
                Iterator<Order> it = Arrays.asList(orders).iterator();

                sb.append("\nORDER BY ");
                while (it.hasNext()) {
                    Order order = it.next();
                    ColumnType ct = relatedTableType.getFieldColumnType(order.getFieldName());
                    sb.append(order.getOrderStatement("_e." + ct.getColumnName()));
                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            String queryString = sb.toString();
            try (PreparedStatement pstm = conn.prepareStatement(queryString)) {

                int i = 1;
                for (PowerColumnType powerValue : whereParams) {
                    powerValue.powerStatement(pstm, i++);
                }
                try (ResultSet rs = pstm.executeQuery()) {
                    while (rs.next()) {
                        Object instance;
                        try {
                            instance = relatedClass.getConstructor().newInstance();
                        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                            Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                            throw new QueryBuilderException(" No se pudo crear instancia de la clase query.getEntityClass() ", ex);
                        }
                        for (PowerColumnType st : select) {
                            st.push(instance, rs);
                        }

                        list.add(instance);
                    }
                }
            }

            System.out.println(" sbQ: " + sb.toString());

        }

        return list;
    }*/
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
                any = st.push(child, rs) || any;
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

        JoinColumnsType jcst = tableType.getJoinColumnsType(fieldName);
        TableType t = getTableType(jcst.getFieldClass(), conn);

        List<ColumnType> selects = t.getColumns();
        List<PowerColumnType> selectColumns = selects.stream().map(e -> {
            PowerColumnType sct = new PowerColumnType(e);
            sct.setColumnAlias(String.format("%s_%s", alias, e.getColumnName()));
            return sct;
        }).collect(Collectors.toList());

        JoinNode node = new JoinNode(alias, jcst, t, selectColumns);

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

    protected String joins(String alias, List<JoinNode> nodes, String schema) {

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

                boolean reverse = node.isReverse();
                String jcName = reverse ? jc.referencedColumnName() : jc.name();
                String referencedColumnName = reverse ? jc.name() : jc.referencedColumnName();

                sb.append(alias).append(".").append(jcName);
                sb.append(" = ");
                sb.append(node.getAlias()).append(".").append(referencedColumnName);

                if (colIt.hasNext()) {
                    sb.append(" AND ");
                }
            }
            sb.append(joins(node.getAlias(), node.getChildren(), schema));
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

    protected List<PowerColumnType> selectAliasIndexator(TableType tableType, String alias) {

        List<PowerColumnType> selectColumns = new ArrayList();

        List<ColumnType> selects = tableType.getColumns();
        selectColumns = selects.stream().map(e -> {
            PowerColumnType sct = new PowerColumnType(e);
            sct.setColumnAlias(String.format("%s_%s", alias, e.getColumnName()));
            return sct;
        }).collect(Collectors.toList());

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

    private RootParametrizedQuery selectParametrizedQuery(Query query, boolean count, Connection conn) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;
        List<CollectionParametrizedQuery> fetchParametrizedQuerys = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        List<JoinNode> rootJoinNodes = toJoinNodes(tableType, query.getJoins(), conn);

        String queryString = "";

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query.getSchema());

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

            List<Fetch> fetchs = query.getFetchs();
            for (Fetch fetch : fetchs) {
                CollectionParametrizedQuery pq = this.selectParametrizedQuery(query, fetch, conn);
                fetchParametrizedQuerys.add(pq);
            }

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
                    PowerColumnType aliasColumnType = getAliasColumnType(order.getFieldName(), query, rootJoinNodes, conn);
                    sb.append(order.getOrderStatement(aliasColumnType.getFullColumnName()));

                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            queryString = sb.toString();
            queryString = getDialect().limit(query.getSchema(), queryString, query.getFirstResult(), query.getMaxResults());
        }

        RootParametrizedQuery pq = new RootParametrizedQuery();
        pq.setQuery(queryString);
        pq.setParams(params);
        pq.setSelectColumns(selectColumns);
        pq.setJoinNodes(rootJoinNodes);
        pq.setFetchs(fetchParametrizedQuerys);
        return pq;
    }

    private CollectionParametrizedQuery selectParametrizedQuery(Query query, Fetch fetch, Connection conn) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        ManyToManyType m2mt = tableType.getManyToManyType(fetch.getCollectionField());
        if (m2mt != null) {

            Class relatedClass = m2mt.getCollectionRelatedClass();
            TableType relatedTableType = getTableType(relatedClass);

            selectColumns = selectAliasIndexator(relatedTableType, fetch.getAlias());

            List<JoinNode> rootJoinNodes = toJoinNodes(relatedTableType, fetch.getJoins(), conn);

            String queryString = "";

            StringBuilder sb = new StringBuilder();
            String joiners = this.joins(fetch.getAlias(), rootJoinNodes, query.getSchema());

            sb.append("SELECT ");

            Iterator<PowerColumnType> selectIt = selectColumns.iterator();
            while (selectIt.hasNext()) {
                PowerColumnType selColumnType = selectIt.next();

                sb.append(fetch.getAlias()).append(".");
                sb.append(selColumnType.getColumnType().getColumnName());
                sb.append(" as ").append(selColumnType.getColumnAlias());

                if (selectIt.hasNext()) {
                    sb.append(", ");
                }
            }

            sb.append(selectJoins(rootJoinNodes));

            sb.append("\nFROM ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }

            sb.append(relatedTableType.getName()).append(" ").append(fetch.getAlias()).append(" ");
            sb.append(joiners);

            JoinTable jt = m2mt.getJoinTable();

            sb.append("\nINNER JOIN ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(jt.name()).append(" tr ON ");

            JoinColumn[] jcs = m2mt.isReverse() ? jt.joinColumns() : jt.inverseJoinColumns();
            Iterator<JoinColumn> jcsit = Arrays.asList(jcs).iterator();
            while (jcsit.hasNext()) {
                JoinColumn jc = jcsit.next();
                sb.append("tr.").append(jc.name()).append(" = ").append(fetch.getAlias()).append(".").append(jc.referencedColumnName());
                if (jcsit.hasNext()) {
                    sb.append(" AND ");
                }
            }

            sb.append("\nINNER JOIN ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(tableType.getName()).append(" _e ON ");

            JoinColumn[] ijcs = m2mt.isReverse() ? jt.inverseJoinColumns() : jt.joinColumns();
            Iterator<JoinColumn> ijcsit = Arrays.asList(ijcs).iterator();
            while (ijcsit.hasNext()) {
                JoinColumn jc = ijcsit.next();
                sb.append("tr.").append(jc.name()).append(" = ").append("_e.").append(jc.referencedColumnName());
                if (ijcsit.hasNext()) {
                    sb.append(" AND ");
                }
            }

            List<ColumnType> ids = tableType.getColumns();
            ids = ids.stream().filter(c -> c.isPrimary()).collect(Collectors.toList());

            // List<PowerColumnType> whereParams = new ArrayList();
            for (ColumnType ct : ids) {
                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(ct.getColumnName());
                params.add(param);
            }

            sb.append("\nWHERE ");
            Iterator<PowerColumnType> colIt = params.iterator();
            while (colIt.hasNext()) {
                PowerColumnType ct = colIt.next();
                if (ct.getColumnType().isPrimary()) {
                    sb.append("_e.").append(ct.getColumnAlias());
                    sb.append(" = ?");
                    //orderParams.add(ct);
                    if (colIt.hasNext()) {
                        sb.append(" AND ");
                    }
                }
            }

            // StringBuilder wheresb = new StringBuilder();
            // sb.append(wheresb);
            if (fetch.getOrders().size() > 0) {
                sb.append("\nORDER BY ");
                Iterator<Order> it = fetch.getOrders().iterator();
                while (it.hasNext()) {
                    Order order = it.next();
                    PowerColumnType aliasColumnType = getAliasColumnType(order.getFieldName(), query, rootJoinNodes, conn);
                    sb.append(order.getOrderStatement(aliasColumnType.getFullColumnName()));

                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            queryString = sb.toString();

            CollectionParametrizedQuery pq = new CollectionParametrizedQuery(fetch.getCollectionField(), relatedClass);
            pq.setQuery(queryString);
            pq.setParams(params);
            pq.setSelectColumns(selectColumns);
            pq.setJoinNodes(rootJoinNodes);

            return pq;
        }

        return null;
    }

    private ParametrizedQuery deleteParametrizedQuery(Query query, Connection conn) throws SQLException, QueryBuilderException {

        List<PowerColumnType> params = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }

        List<JoinNode> rootJoinNodes = toJoinNodes(tableType, query.getJoins(), conn);

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query.getSchema());

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

        ParametrizedQuery pq = new ParametrizedQuery();
        pq.setQuery(queryString);
        pq.setParams(params);
        pq.setJoinNodes(rootJoinNodes);
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
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query.getSchema());
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
        ParametrizedQuery pq = new ParametrizedQuery();
        pq.setQuery(queryString);
        pq.setParams(params);
        pq.setJoinNodes(rootJoinNodes);
        return pq;
    }

    public String getMetadaSchema() {
        return metadaSchema;
    }

    public void setMetadaSchema(String metadaSchema) {
        this.metadaSchema = metadaSchema;
    }
}
