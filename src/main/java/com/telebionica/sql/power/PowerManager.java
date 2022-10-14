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
import com.telebionica.sql.predicates.Junction;
import com.telebionica.sql.predicates.Not;
import com.telebionica.sql.predicates.Predicate;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.CollectionParametrizedQuery;
import com.telebionica.sql.query.Fetch;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.PowerQueryException;
import com.telebionica.sql.query.SelectParametrizedQuery;
import com.telebionica.sql.setu.SetForUpdate;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.GeneratorType;
import com.telebionica.sql.type.ManyToManyType;
import com.telebionica.sql.type.JoinColumnsType;
import com.telebionica.sql.type.TableType;
import com.telebionica.sql.util.Generator;
import com.telebionica.validator.ann.AnnotationUtil;
import com.telebionica.validator.Message;
import com.telebionica.validator.Messages;
import com.telebionica.validator.Validator;
import com.telebionica.validator.ann.Unique;
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
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 *
 * @author aldo
 *
 */
public abstract class PowerManager {

    private String metadaSchema;
    private boolean debugSQL = Boolean.FALSE;
    private boolean debugConfig = Boolean.FALSE;
    private final Map<Class, TableType> tableTypeMap = new HashMap<>();
    private final Map<String, Generator> generatorMap = new HashMap<>();
    private Dialect dialect = null;

    private static final Logger logger = Logger.getLogger(PowerManager.class.getName());

    public abstract Connection getConnection() throws PowerQueryException;

    public <E> Query createQuery() throws PowerQueryException {
        Query<E> query = new Query(this);
        return query;
    }

    public int insert(Object e) throws PowerQueryException {
        return insert(null, e);
    }

    public int insert(String schema, Object e) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return insert(schema, e, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public int insert(String schema, Object e, Connection conn) throws PowerQueryException {

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
                    try ( PreparedStatement pstm = conn.prepareStatement(seqQueryString);  ResultSet rs = pstm.executeQuery()) {
                        if (rs.next()) {
                            PowerColumnType secpow = new PowerColumnType(ct);
                            secpow.setColumnAlias(ct.getColumnName());
                            secpow.push(e, rs, 1);
                            secpow.getter(e);
                            insertParams.add(secpow);
                        }
                    } catch (SQLException ex) {
                        throw new PowerQueryException(seqQueryString, ex);
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

        if (debugSQL) {
            String q = logs(query, insertParams);
            logger.log(Level.INFO, q);
        }

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
                try ( ResultSet rs = pstm.getGeneratedKeys()) {
                    int index = 1;
                    for (PowerColumnType auto : autos) {
                        if (rs.next()) {
                            auto.push(e, rs, index++);
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            String q = logs(query, insertParams);
            throw new PowerQueryException(q, ex);
        } finally {
            if (pstm != null) {
                try {
                    pstm.close();
                } catch (Exception ex) {
                    throw new PowerQueryException(ex);
                }
            }
        }

        return 0;
    }

    public List<Messages> validateToInsert(Object e, Class<?> group) throws PowerQueryException {
        return validateToInsert(null, e, group);
    }

    public List<Messages> validateToInsert(String schema, Object e, Class<?> group) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return validateToInsert(schema, e, group, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public List<Messages> validateToInsert(String schema, Object e, Class<?> group, Connection conn) throws PowerQueryException {
        return validate(schema, e, group, false, conn);
    }

    public List<Messages> validateToUpdate(Object e, Class<?> group) throws PowerQueryException {
        return validateToUpdate(null, e, group);
    }

    public List<Messages> validateToUpdate(String schema, Object e, Class<?> group) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return validateToUpdate(schema, e, group, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public List<Messages> validateToUpdate(String schema, Object e, Class<?> group, Connection conn) throws PowerQueryException {
        return validate(schema, e, group, true, conn);
    }

    public List<Messages> validate(String schema, Object e, Class<?> group, boolean exclusive, Connection conn) throws PowerQueryException {

        List<Message> rootMessages = new ArrayList<Message>();
        List<Messages> messagesList = new ArrayList();

        Class<?> theclass = e.getClass();
        TableType tableType = this.getTableType(theclass, conn);

        Field[] scopeFields = theclass.getDeclaredFields();
        for (Field scopeField : scopeFields) {

            if (AnnotationUtil.tieneUnico(scopeField, group)) {

                Unique ann = scopeField.getAnnotation(Unique.class);
                List<Message> msgs = new ArrayList<Message>();

                Not not = null;

                if (exclusive) {
                    not = new Not(Junction.JUNCTION_TYPE.AND);
                    List<ColumnType> ids = tableType.getIdColumns();
                    // tableType.getColumns();
                    // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());
                    

                    List<PowerColumnType> whereParams = new ArrayList();
                    for (ColumnType ct : ids) {
                        PowerColumnType param = new PowerColumnType(ct);
                        param.setColumnAlias(ct.getColumnName());
                        param.getter(e);
                        whereParams.add(param);

                        not.add(Predicates.eq(ct.getFieldName(), param.getValue()));
                    }

                }

                ColumnType ct = tableType.getFieldColumnType(scopeField.getName());

                PowerColumnType param = new PowerColumnType(ct);
                param.getter(e);

                Query query = createQuery();
                query.schema(schema).select().
                        from(theclass);
                query.where(Predicates.eq(scopeField.getName(), param.getValue()));
                if (not != null) {
                    query.and(not);
                }

                int c = query.count(conn);

                if (c > 0) {

                    String key = Validator.noramalizeKeyMessage(ann.message(), e.getClass().getSimpleName(), scopeField.getName(), "Unique");
                    msgs.add(new Message(key));

                    Messages messages = new Messages(scopeField.getName(), msgs);
                    messagesList.add(messages);
                }

                System.out.println(" C: " + c);
            }

        }

        if (!rootMessages.isEmpty()) {
            Messages messages = new Messages("root", rootMessages);
            messagesList.add(messages);
        }

        return messagesList;
    }

    public <E> List<E> transform(String query, Class<E> target, TRANSFORMTYPE transformtype, Function<PreparedStatement, Void> fun, Connection conn) throws PowerQueryException {

        TableType tableType = getTableType(target);

        List<PowerColumnType> columns = new ArrayList();

        List<E> list = new ArrayList();
        try ( PreparedStatement pstmt = conn.prepareStatement(query)) {

            if (fun != null) {
                fun.apply(pstmt);
            }

            try ( ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();
                for (int index = 1; index <= columnCount; index++) {
                    String columnName = md.getColumnLabel(index);

                    ColumnType ct = null;
                    if (transformtype == null || transformtype == TRANSFORMTYPE.COLUMNNAME) {
                        ct = tableType.getColumnType(columnName);
                    } else if (transformtype == TRANSFORMTYPE.FIELDNAME) {
                        ct = tableType.getFieldColumnType(columnName);
                    }

                    if (ct != null) {
                        PowerColumnType pw = new PowerColumnType(ct);
                        pw.setColumnAlias(columnName);
                        columns.add(pw);
                    }
                }

                try {
                    while (rs.next()) {
                        E instance = target.getConstructor().newInstance();
                        for (PowerColumnType pct : columns) {
                            pct.push(instance, rs);
                        }
                        list.add(instance);
                    }
                } catch (PowerQueryException | IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException | SQLException ex) {
                    throw new PowerQueryException(ex);
                }
            }
        } catch (SQLException ex) {
            String q = logs(query, fun);
            throw new PowerQueryException(q, ex);
        }
        return list;
    }

    public <E> List<E> transform(String query, TRANSFORMTYPE transformtype, Class<E> target) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return transform(query, target, transformtype, null, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public <E> List<E> transform(String query, TRANSFORMTYPE transformtype, Function<PreparedStatement, Void> fun, Class<E> target) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return transform(query, target, transformtype, fun, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public <E> List<E> transform(String query, Function<PreparedStatement, Void> fun, Class<E> target) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return transform(query, target, null, fun, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public <E> List<E> transform(String query, Class<E> target) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return transform(query, target, null, null, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    private Generator getGenerator(GeneratorType gt) throws PowerQueryException {

        Generator gen = generatorMap.get(gt.getName());
        if (gen == null) {
            try {
                Class<Generator> gclass = (Class<Generator>) Class.forName(gt.getGenerator());
                gen = gclass.getConstructor().newInstance();
                generatorMap.put(gt.getName(), gen);
            } catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                throw new PowerQueryException("Generador " + gt.getGenerator() + " invalido", ex);
            }
        }

        return gen;
    }

    public <E> int replace(String schema, Object ent, String manyToManyField, List<E> list) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return replace(schema, ent, manyToManyField, list, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public <E> int replace(String schema, Object ent, String manyToManyField, List<E> list, Connection conn) throws PowerQueryException {

        if (ent == null) {
            return 0;
        }

        Class entityClass = ent.getClass();
        TableType tableType = getTableType(entityClass);

        ManyToManyType m2mt = tableType.getManyToManyType(manyToManyField);
        if (m2mt == null) {
            throw new PowerQueryException("No esta el atributo " + manyToManyField + " en la clase " + entityClass.getName());
        }

        JoinTable jt = m2mt.getJoinTable();
        JoinColumn[] jcs = m2mt.isReverse() ? jt.inverseJoinColumns() : jt.joinColumns();

        List<PowerColumnType> deleteParams = new ArrayList();

        for (JoinColumn jc : jcs) {
            ColumnType ct = tableType.getColumnType(jc.referencedColumnName());
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(jc.name());
            param.getter(ent);
            deleteParams.add(param);
            // insertParams.add(param);
        }

        StringBuilder deletesb = new StringBuilder("DELETE FROM ");

        if (schema != null) {
            deletesb.append(schema).append(".");
        }

        deletesb.append(jt.name()).append(" ");
        deletesb.append("\nWHERE ");

        Iterator<PowerColumnType> colIt = deleteParams.iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            deletesb.append(ct.getColumnAlias());
            deletesb.append(" = ?");
            if (colIt.hasNext()) {
                deletesb.append(" AND ");
            }
        }

        // System.out.println(" DELETE QUERY: " + deletesb.toString());
        StringBuilder insertsb = new StringBuilder("INSERT INTO ");

        if (schema != null) {
            insertsb.append(schema).append(".");
        }

        insertsb.append(jt.name()).append(" (");

        Class relatedClass = m2mt.getCollectionRelatedClass();
        TableType relatedTableType = getTableType(relatedClass);

        List<PowerColumnType> insertParams = new ArrayList();
        JoinColumn[] ijcs = m2mt.isReverse() ? jt.joinColumns() : jt.inverseJoinColumns();
        for (JoinColumn jc : ijcs) {
            ColumnType ct = relatedTableType.getColumnType(jc.referencedColumnName());
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(jc.name());
            insertParams.add(param);
        }

        List<PowerColumnType> fullParams = new ArrayList();
        fullParams.addAll(deleteParams);
        fullParams.addAll(insertParams);

        Iterator<PowerColumnType> icolIt = fullParams.iterator();
        StringBuilder sbvalues = new StringBuilder();
        while (icolIt.hasNext()) {
            PowerColumnType ct = icolIt.next();
            insertsb.append(ct.getColumnAlias());
            sbvalues.append("?");

            if (icolIt.hasNext()) {
                insertsb.append(", ");
                sbvalues.append(", ");
            }
        }

        insertsb.append(" ) VALUES(");
        insertsb.append(sbvalues);
        insertsb.append(")");

        if (debugSQL) {
            String q = logs(deletesb.toString(), deleteParams);
            logger.log(Level.INFO, q);
        }

        try ( PreparedStatement delStmt = conn.prepareStatement(deletesb.toString())) {
            int i = 1;
            for (PowerColumnType powerValue : deleteParams) {
                powerValue.powerStatement(delStmt, i++);
            }
            delStmt.executeUpdate();
        } catch (SQLException ex) {
            String q = logs(deletesb.toString(), deleteParams);
            throw new PowerQueryException(q, ex);
        }

        if (debugSQL) {
            String q = logs(insertsb.toString(), fullParams);
            logger.log(Level.INFO, q);
        }
        try ( PreparedStatement insertStmt = conn.prepareStatement(insertsb.toString())) {

            for (E obj : list) {
                for (PowerColumnType pct : insertParams) {
                    pct.getter(obj);
                }

                int i = 1;
                for (PowerColumnType full : fullParams) {
                    full.powerStatement(insertStmt, i++);
                }
                insertStmt.addBatch();
            }
            insertStmt.executeBatch();
        } catch (SQLException ex) {
            String q = logs(insertsb.toString(), fullParams);
            throw new PowerQueryException(q, ex);
        }
        // System.out.println(" INSRET QUERY: " + insertsb.toString());

        return 0;
    }

    public int update(Object e, String... fields) throws PowerQueryException {
        return update(null, e, fields);
    }

    public int update(String schema, Object e, String... fields) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return update(schema, e, conn, fields);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public int update(String schema, Object e, Connection conn, String... fields) throws PowerQueryException {

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
        Iterator<PowerColumnType> colIt = updateParams.stream().filter(c -> !(c.getColumnType().isPrimary() || c.getColumnType().isEmbedded())).collect(Collectors.toList()).iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            sb.append(ct.getColumnAlias());
            sb.append(" = ?");
            orderParams.add(ct);
            if (colIt.hasNext()) {
                sb.append(", ");

            }
        }

        colIt = updateParams.stream().filter(c -> c.getColumnType().isPrimary() || c.getColumnType().isEmbedded()).collect(Collectors.toList()).iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            sbvalues.append(ct.getColumnAlias());
            sbvalues.append(" = ?");
            orderParams.add(ct);
            if (colIt.hasNext()) {
                sbvalues.append(" AND ");

            }
        }

        sb.append(" WHERE ");
        sb.append(sbvalues);

        String query = sb.toString();

        // System.out.println(" UPDATE QUERY: " + query);
        int c;
        try ( PreparedStatement pstm = conn.prepareStatement(query)) {

            int i = 1;
            for (PowerColumnType powerValue : orderParams) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        } catch (SQLException ex) {
            String q = logs(query, orderParams);
            throw new PowerQueryException(q, ex);
        }

        return c;
    }

    public int delete(Object e) throws PowerQueryException {
        return delete(null, e);
    }

    public int delete(String schema, Object e) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return delete(schema, e, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public int delete(String schema, Object e, Connection conn) throws PowerQueryException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

        List<ColumnType> ids = tableType.getIdColumns();
        // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());

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
            if (ct.getColumnType().isPrimary() || ct.getColumnType().isEmbedded()) {
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

        // System.out.println(" DELETE QUERY: " + queryString);
        if (debugSQL) {
            String q = logs(queryString, orderParams);
            logger.log(Level.INFO, q);
        }
        int c;
        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : orderParams) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        } catch (SQLException ex) {
            String q = logs(queryString, orderParams);
            throw new PowerQueryException(q, ex);
        }
        return c;
    }

    public void refresh(Object e, String... columnNames) throws PowerQueryException {
        refresh(null, e, columnNames);
    }

    public void refresh(String schema, Object e, String... columnNames) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            refresh(schema, e, conn, columnNames);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public void refresh(String schema, Object e, Connection conn, String... columnNames) throws PowerQueryException {

        Class entityClass = e.getClass();
        TableType tableType = this.getTableType(entityClass, conn);

        List<ColumnType> selects = tableType.getFilterColumns(columnNames);

        List<PowerColumnType> selectColumns = selects.stream().map(c -> {
            PowerColumnType sct = new PowerColumnType(c);
            sct.setColumnAlias(String.format("%s_%s", "e", c.getColumnName()));
            return sct;
        }).collect(Collectors.toList());

        List<ColumnType> ids = tableType.getIdColumns();
        // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());

        List<PowerColumnType> whereParams = new ArrayList();
        for (ColumnType ct : ids) {
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(ct.getColumnName());
            param.getter(e);
            whereParams.add(param);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        Iterator<PowerColumnType> selectIt = selectColumns.iterator();
        while (selectIt.hasNext()) {
            PowerColumnType selColumnType = selectIt.next();

            sb.append("e.");
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

        sb.append(tableType.getName());
        sb.append(" e");

        sb.append("\nWHERE ");

        Iterator<PowerColumnType> colIt = whereParams.iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            if (ct.getColumnType().isPrimary() || ct.getColumnType().isEmbedded()) {
                sb.append(ct.getColumnAlias());
                sb.append(" = ?");
                if (colIt.hasNext()) {
                    sb.append(" AND ");
                }
            }
        }

        String queryString = sb.toString();

        if (debugSQL) {
            String q = logs(queryString, whereParams);
            logger.log(Level.INFO, q);
        }

        // System.out.println(" refresh QUERY: " + queryString);
        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : whereParams) {
                powerValue.powerStatement(pstm, i++);
            }
            try ( ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    for (PowerColumnType st : selectColumns) {
                        st.push(e, rs);
                    }
                }
            }
        } catch (SQLException ex) {
            String q = logs(queryString, whereParams);
            throw new PowerQueryException(q, ex);
        }
    }

    public <E> E get(Serializable id, Class<E> entityClass, String... columnNames) throws PowerQueryException {
        return get(null, id, entityClass, columnNames);
    }

    public <E> E get(String schema, Serializable id, Class<E> entityClass, String... columnNames) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return get(schema, id, entityClass, conn, columnNames);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public <E> E get(Serializable id, Class<E> entityClass, Connection conn, String... columnNames) throws PowerQueryException {
        return get(null, id, entityClass, conn, columnNames);
    }

    public <E> E get(String schema, Serializable id, Class<E> entityClass, Connection conn, String... columnNames) throws PowerQueryException {

        TableType tableType = this.getTableType(entityClass, conn);

        List<ColumnType> selects = tableType.getFilterColumns(columnNames);

        List<PowerColumnType> selectColumns = selects.stream().map(c -> {
            PowerColumnType sct = new PowerColumnType(c);
            sct.setColumnAlias(String.format("%s_%s", "e", c.getColumnName()));
            return sct;
        }).collect(Collectors.toList());

        List<ColumnType> ids = tableType.getIdColumns();
        // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());

        List<PowerColumnType> whereParams = new ArrayList();
        for (ColumnType ct : ids) {
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(ct.getColumnName());
            // param.getter(e);
            param.setValue(id);
            whereParams.add(param);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        Iterator<PowerColumnType> selectIt = selectColumns.iterator();
        while (selectIt.hasNext()) {
            PowerColumnType selColumnType = selectIt.next();

            sb.append("e.");
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

        sb.append(tableType.getName());
        sb.append(" e");

        sb.append("\nWHERE ");

        Iterator<PowerColumnType> colIt = whereParams.iterator();
        while (colIt.hasNext()) {
            PowerColumnType ct = colIt.next();
            if (ct.getColumnType().isPrimary() || ct.getColumnType().isEmbedded()) {
                sb.append(ct.getColumnAlias());
                sb.append(" = ?");
                if (colIt.hasNext()) {
                    sb.append(" AND ");
                }
            }
        }

        String queryString = sb.toString();

        if (debugSQL) {
            String q = logs(queryString, whereParams);
            logger.log(Level.INFO, q);
        }
        // System.out.println(" refresh QUERY: " + queryString);
        E e = null;

        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : whereParams) {
                powerValue.powerStatement(pstm, i++);
            }
            try ( ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    e = entityClass.getConstructor().newInstance();
                    for (PowerColumnType st : selectColumns) {
                        st.push(e, rs);
                    }
                }
            } catch (Exception ex) {
                throw new PowerQueryException(ex);
            }
        } catch (SQLException ex) {
            String q = logs(queryString, whereParams);
            throw new PowerQueryException(q, ex);
        }

        return e;
    }

    public synchronized TableType getTableType(Class entityClass) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return getTableType(entityClass, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public synchronized TableType getTableType(Class entityClass, Connection conn) throws PowerQueryException {

        TableType tableType = tableTypeMap.get(entityClass);
        if (tableType == null) {
            tableType = buildTableType(entityClass, conn);
            tableTypeMap.put(entityClass, tableType);
        }
        return tableType;
    }

    private <E> TableType buildTableType(Class<E> entityClass, Connection conn) throws PowerQueryException {

        Table ann = (Table) entityClass.getDeclaredAnnotation(Table.class);
        if (ann == null) {
            throw new PowerQueryException("No existe la anotacion @Table en la clase " + entityClass);
        }

        String tableNeme = ann.name();
        TableType tableType = new TableType(tableNeme, entityClass);

        List<ColumnType> columns = new ArrayList();
        List<JoinColumnsType> joinColumnsTypes = new ArrayList();
        List<ManyToManyType> manyToManyTypes = new ArrayList();
        List<JoinColumnsType> oneToManyTypes = new ArrayList();

        Field[] scopeFields = entityClass.getDeclaredFields();

        for (Field field : scopeFields) {
            addColumnType(field, columns, tableType);
            addJoinColumnsType(field, joinColumnsTypes, tableType);
            addManyToManyType(field, manyToManyTypes, tableType);
            addOneToManyType(field, oneToManyTypes, tableType);
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

        try ( PreparedStatement pst = conn.prepareStatement(query);  ResultSet rs = pst.executeQuery()) {
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
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }

        tableType.setColumns(columns);
        tableType.setJoinColumns(joinColumnsTypes);
        tableType.setManyToManyTypes(manyToManyTypes);
        tableType.setOneToManyTypes(oneToManyTypes);

        if (debugConfig) {
            Gson gson = new GsonBuilder()
                    .excludeFieldsWithModifiers(Modifier.PROTECTED)
                    .setPrettyPrinting().create();
            String json = gson.toJson(tableType);
            System.out.println("TablaType " + json);
        }

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

        EmbeddedId eidann = field.getAnnotation(EmbeddedId.class);

        if (eidann != null) {

            Field[] idfields = field.getType().getDeclaredFields();

            for (Field farid : idfields) {

                Column farann = farid.getAnnotation(Column.class);
                if (farann != null) {
                    ColumnType ct = new ColumnType(farann.name(), farid.getName(), farid.getType(), tableType);
                    ct.setPrimary(false);
                    ct.setEmbedded(true);
                    ct.setEmbeddedFieldName(field.getName());
                    ct.setEmbeddedFieldClass(field.getType());
                    columns.add(ct);
                }
            }
        }
    }

    private void addOneToManyType(Field field, List<JoinColumnsType> oneToManyTypes, TableType tableType) throws PowerQueryException {

        OneToMany o2m = field.getAnnotation(OneToMany.class);
        if (o2m != null) {

            Class collectionRelatedClass = getCollectionRelatedClass(field);
            if (collectionRelatedClass == null) {
                throw new PowerQueryException("No se logro obtener la clase relacionada, utilice notacion generica <Ent>" + field.getName());
            }

            try {

                String mbyFieldName = o2m.mappedBy();
                Field relatedField = collectionRelatedClass.getDeclaredField(mbyFieldName);
                List<JoinColumn> joiners = getJoiners(relatedField);

                JoinColumnsType cjt = new JoinColumnsType(field.getName(), collectionRelatedClass, joiners, tableType);
                oneToManyTypes.add(cjt);

            } catch (NoSuchFieldException | SecurityException e) {
                throw new PowerQueryException("No se logro obtener reverso manyToMany del atributo " + field.getName(), e);
            }
        }
    }

    private void addManyToManyType(Field field, List<ManyToManyType> manyToManyTypes, TableType tableType) throws PowerQueryException {

        ManyToMany m2m = field.getAnnotation(ManyToMany.class);
        if (m2m != null) {

            Class collectionRelatedClass = getCollectionRelatedClass(field);
            if (collectionRelatedClass == null) {
                throw new PowerQueryException("No se logro obtener la clase relacionada, utilice notacion generica <Ent>" + field.getName());
            }
            try {

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
            } catch (NoSuchFieldException | SecurityException e) {
                throw new PowerQueryException("No se logro obtener reverso manyToMany del atributo " + field.getName(), e);
            }
        }

    }

    private Class getCollectionRelatedClass(Field field) throws PowerQueryException {

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
                    throw new PowerQueryException("No se logro obtener la clase relacionada, utilice notacion generica <Ent>" + field.getName(), e);
                }
            }
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

    private void addJoinColumnsType(Field field, List<JoinColumnsType> joinColumnsTypes, TableType tableType) throws PowerQueryException {

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
                    throw new PowerQueryException("No se logro obtener reverso manyToMany del atributo " + field.getName(), e);
                }
            } else {
                List<JoinColumn> joiners = getJoiners(field);
                JoinColumnsType cjt = new JoinColumnsType(field.getName(), field.getType(), joiners, tableType);
                joinColumnsTypes.add(cjt);
            }
        }
    }

    public Dialect getDialect() throws PowerQueryException {
        if (dialect == null && this.getClass().isAnnotationPresent(Power.class)) {
            try {
                Power ann = this.getClass().getAnnotation(Power.class);
                Class<? extends Dialect> theclass = ann.dialect();
                dialect = theclass.getConstructor(PowerManager.class).newInstance(this);
            } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
                throw new PowerQueryException("No se ha definido un dialect valido ", e);
            }
        }

        return dialect;
    }

    public <E> E unique(Query<E> query) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return unique(query, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    private <E> E unique(Query<E> query, Connection conn) throws PowerQueryException {
        List<E> list = list(query, true, conn);
        if (!list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    public <E> List<E> list(Query<E> query) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return list(query, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    private <E> List<E> list(Query<E> query, Connection conn) throws PowerQueryException {
        return list(query, false, conn);
    }

    private <E> List<E> list(Query<E> query, boolean unique, Connection conn) throws PowerQueryException {
        List<E> list = new ArrayList();

        SelectParametrizedQuery<E> parametrizedQuery = (SelectParametrizedQuery) dryRun(query, conn);
        String queryString = parametrizedQuery.getQuery();

        if (debugSQL) {
            String q = logs(queryString, parametrizedQuery.getParams());
            logger.log(Level.INFO, q);
        }

        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : parametrizedQuery.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                int k = 0;
                while (rs.next()) {
                    if (unique && k > 0) {
                        throw new PowerQueryException(" Resultado no unico ");
                    }
                    E rootInstance;
                    try {
                        rootInstance = parametrizedQuery.getTargetClass().getConstructor().newInstance();
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                        Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                        throw new PowerQueryException(" No se pudo crear instancia de la clase query.getEntityClass() ", ex);
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
                    k++;
                }
            }
        } catch (SQLException ex) {
            String q = logs(queryString, parametrizedQuery.getParams());
            throw new PowerQueryException(q, ex);
        }

        // Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        // String json = gson.toJson(list);
        // System.out.println(" LIST: " + json);
        return list;
    }

    private List getCollection(Object rootInstance, CollectionParametrizedQuery pq, Connection conn) throws PowerQueryException {
        if (debugSQL) {
            String q = logs(pq.getQuery(), pq.getParams());
            logger.log(Level.INFO, q);
        }
        List fetchList = new ArrayList();
        try ( PreparedStatement pstm = conn.prepareStatement(pq.getQuery())) {

            int j = 1;
            for (PowerColumnType pct : pq.getParams()) {
                pct.getter(rootInstance);
                pct.powerStatement(pstm, j++);
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                while (rs.next()) {
                    Object instance;
                    try {
                        instance = pq.getTargetClass().getConstructor().newInstance();
                    } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
                        Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                        throw new PowerQueryException(" No se pudo crear instancia de la clase query.getEntityClass() ", ex);
                    }
                    for (PowerColumnType st : pq.getSelectColumns()) {
                        st.push(instance, rs);
                    }
                    push(instance, pq.getJoinNodes(), rs);
                    fetchList.add(instance);
                }
            }

        } catch (SQLException ex) {
            String q = logs(pq.getQuery(), pq.getParams());
            throw new PowerQueryException(q, ex);
        }
        return fetchList;
    }

    public void push(Class targetClass, String fieldName, Object target, List collection) throws PowerQueryException {
        try {
            getWriteMethod(targetClass, fieldName).invoke(target, collection);
        } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new PowerQueryException(e);
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

    public Integer count(Query query) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return count(query, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public Integer count(Query query, Connection conn) throws PowerQueryException {

        int c = 0;

        ParametrizedQuery pq = dryRun(query, true, conn);
        String queryString = pq.getQuery();
        if (debugSQL) {
            String q = logs(queryString, pq.getParams());
            logger.log(Level.INFO, q);
        }

        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }

            try ( ResultSet rs = pstm.executeQuery()) {
                if (rs.next()) {
                    c = rs.getInt(1);
                }
            }
        } catch (SQLException ex) {
            String q = logs(pq.getQuery(), pq.getParams());
            throw new PowerQueryException(q, ex);
        }
        return c;
    }

    public int execute(Query query) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return execute(query, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
        }
    }

    public int execute(Query query, Connection conn) throws PowerQueryException {

        int c;
        ParametrizedQuery pq = dryRun(query, conn);
        String queryString = pq.getQuery();

        if (debugSQL) {
            String q = logs(queryString, pq.getParams());
            logger.log(Level.INFO, q);
        }
        try ( PreparedStatement pstm = conn.prepareStatement(queryString)) {

            int i = 1;
            for (PowerColumnType powerValue : pq.getParams()) {
                powerValue.powerStatement(pstm, i++);
            }
            c = pstm.executeUpdate();
        } catch (SQLException ex) {
            String q = logs(pq.getQuery(), pq.getParams());
            throw new PowerQueryException(q, ex);
        }

        return c;
    }

    private void push(Object parent, List<JoinNode> nodes, ResultSet rs) throws PowerQueryException {

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

    public List<JoinNode> toJoinNodes(TableType tableType, List<Join> joins, Connection conn) throws PowerQueryException {

        List<JoinNode> rootJoinNodes = new ArrayList();
        for (Join j : joins) {

            String[] path = j.getFieldPath().split("[.]");
            if (path.length <= 0) {
                throw new PowerQueryException("No se encuentra el path " + j.getFieldPath());
            }

            joins(path, j.getAlias(), rootJoinNodes, tableType, j.getJointype(), conn);
        }
        return rootJoinNodes;
    }

    private void joins(String[] path, String alias, List<JoinNode> joins, TableType tableType, Query.JOINTYPE joinType, Connection conn) throws PowerQueryException {

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

    private JoinNode getJoinNode(TableType tableType, String fieldName, String alias, Connection conn) throws PowerQueryException {

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

    /*public PowerColumnType getAliasColumnType(String fieldPath, Query query, List<JoinNode> rootJoinNodes) throws SQLException, PowerQueryException {
        try (Connection conn = getConnection()) {
            return getAliasColumnType(fieldPath, query, rootJoinNodes, conn);
        }
    }*/
    public PowerColumnType getAliasColumnType(String fieldPath, Class entityClass, String rootAlias, List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {

        TableType tableType = getTableType(entityClass, conn);

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
            ext.setTableAlias(rootAlias);
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

    public ParametrizedQuery dryRun(Query query, boolean count, Connection conn) throws PowerQueryException {

        ParametrizedQuery pq = null;
        if (query.getQtype() == Query.QTYPE.SELECT) {
            pq = selectParametrizedQuery(query, count, conn);
        } else if (query.getQtype() == Query.QTYPE.DELETE) {
            pq = deleteParametrizedQuery(query, conn);
        } else if (query.getQtype() == Query.QTYPE.UPDATE) {
            pq = updateParametrizedQuery(query, conn);
        }

        return pq;
    }

    public ParametrizedQuery dryRun(Query query, Connection conn) throws PowerQueryException {
        return dryRun(query, false, conn);
    }

    public ParametrizedQuery dryRun(Query query) throws PowerQueryException {
        return dryRun(query, false);
    }

    public ParametrizedQuery dryRun(Query query, boolean count) throws PowerQueryException {
        try ( Connection conn = getConnection()) {
            return dryRun(query, count, conn);
        } catch (SQLException ex) {
            throw new PowerQueryException(ex);
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

    private SelectParametrizedQuery selectParametrizedQuery(Query query, boolean count, Connection conn) throws PowerQueryException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;
        List<CollectionParametrizedQuery> fetchParametrizedQuerys = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new PowerQueryException("No esta definida la tabla en donde construir el query");
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
                CollectionParametrizedQuery pq = this.fetchParametrizedQuery(query, fetch, conn);
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
                    PowerColumnType aliasColumnType = getAliasColumnType(order.getFieldName(), query.getEntityClass(), query.getRootAlias(), rootJoinNodes, conn);
                    sb.append(order.getOrderStatement(aliasColumnType.getFullColumnName()));

                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            queryString = sb.toString();
            queryString = getDialect().limit(query.getSchema(), queryString, query.getFirstResult(), query.getMaxResults());
        }

        SelectParametrizedQuery pq = new SelectParametrizedQuery(query.getEntityClass());
        pq.setQuery(queryString);
        pq.setParams(params);
        pq.setSelectColumns(selectColumns);
        pq.setJoinNodes(rootJoinNodes);
        pq.setFetchs(fetchParametrizedQuerys);

        // System.out.println("ROOT QUERY: " + queryString);
        // System.out.println("");
        return pq;
    }

    private CollectionParametrizedQuery fetchParametrizedQuery(Query query, Fetch fetch, Connection conn) throws PowerQueryException {

        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new PowerQueryException("No esta definida la tabla en donde construir el query");
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

            List<ColumnType> ids = tableType.getIdColumns();
            // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());

            for (ColumnType ct : ids) {
                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(ct.getColumnName());
                params.add(param);
            }

            sb.append("\nWHERE ");
            Iterator<PowerColumnType> colIt = params.iterator();
            while (colIt.hasNext()) {
                PowerColumnType ct = colIt.next();
                if (ct.getColumnType().isPrimary() || ct.getColumnType().isEmbedded()) {
                    sb.append("_e.").append(ct.getColumnAlias());
                    sb.append(" = ?");
                    if (colIt.hasNext()) {
                        sb.append(" AND ");
                    }
                }
            }

            if (fetch.getOrders().size() > 0) {
                sb.append("\nORDER BY ");
                Iterator<Order> it = fetch.getOrders().iterator();
                while (it.hasNext()) {
                    Order order = it.next();
                    PowerColumnType aliasColumnType = getAliasColumnType(order.getFieldName(), relatedClass, fetch.getAlias(), rootJoinNodes, conn);
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
            // System.out.println("FETH QUERY: " + queryString);
            // System.out.println("");
            return pq;
        }

        JoinColumnsType jct = tableType.getOneToManyType(fetch.getCollectionField());
        if (jct != null) {
            Class relatedClass = jct.getFieldClass();
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

            sb.append("\nINNER JOIN ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(tableType.getName()).append(" _e ON ");

            Iterator<JoinColumn> ijcsit = jct.getJoiners().iterator();
            while (ijcsit.hasNext()) {
                JoinColumn jc = ijcsit.next();
                sb.append(fetch.getAlias()).append(".").append(jc.name()).append(" = _e.").append(jc.referencedColumnName());
                if (ijcsit.hasNext()) {
                    sb.append(" AND ");
                }
            }

            List<ColumnType> ids = tableType.getIdColumns();
            // ids = ids.stream().filter(c -> c.isPrimary() || c.isEmbedded()).collect(Collectors.toList());

            for (ColumnType ct : ids) {
                PowerColumnType param = new PowerColumnType(ct);
                param.setColumnAlias(ct.getColumnName());
                params.add(param);
            }

            sb.append("\nWHERE ");
            Iterator<PowerColumnType> colIt = params.iterator();
            while (colIt.hasNext()) {
                PowerColumnType ct = colIt.next();
                if (ct.getColumnType().isPrimary() || ct.getColumnType().isEmbedded()) {
                    sb.append("_e.").append(ct.getColumnAlias());
                    sb.append(" = ?");
                    if (colIt.hasNext()) {
                        sb.append(" AND ");
                    }
                }
            }

            if (fetch.getOrders().size() > 0) {
                sb.append("\nORDER BY ");
                Iterator<Order> it = fetch.getOrders().iterator();
                while (it.hasNext()) {
                    Order order = it.next();
                    PowerColumnType aliasColumnType = getAliasColumnType(order.getFieldName(), relatedClass, fetch.getAlias(), rootJoinNodes, conn);
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
            // System.out.println("FETH QUERY: " + queryString);
            // System.out.println("");
            return pq;
        }

        return null;
    }

    private ParametrizedQuery deleteParametrizedQuery(Query query, Connection conn) throws PowerQueryException {

        List<PowerColumnType> params = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new PowerQueryException("No esta definida la tabla en donde construir el query");
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

    private ParametrizedQuery updateParametrizedQuery(Query query, Connection conn) throws PowerQueryException {
        List<PowerColumnType> params = new ArrayList();

        TableType tableType = getTableType(query.getEntityClass(), conn);

        if (tableType == null) {
            throw new PowerQueryException("No esta definida la tabla en donde construir el query");
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

    public boolean isDebugSQL() {
        return debugSQL;
    }

    public void setDebugSQL(boolean debugSQL) {
        this.debugSQL = debugSQL;
    }

    public boolean isDebugConfig() {
        return debugConfig;
    }

    public void setDebugConfig(boolean debugConfig) {
        this.debugConfig = debugConfig;
    }

    private String logs(String query, List<PowerColumnType> params) {

        StringBuilder sb = new StringBuilder();
        sb.append(query);

        if (params != null && !params.isEmpty()) {
            sb.append("\n  ");
            String vals = params.stream().map(p -> p.getValue() == null ? "[NULL]" : p.getValue().toString()).collect(Collectors.joining(", "));
            sb.append(vals);
        }

        return sb.toString();
    }

    private String logs(String query, Function<PreparedStatement, Void> fun) {

        StringBuilder sb = new StringBuilder();
        sb.append(query);

        sb.append("Function: ");
        sb.append(fun.toString());

        return sb.toString();
    }
}
