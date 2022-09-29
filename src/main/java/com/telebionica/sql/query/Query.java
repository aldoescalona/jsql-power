/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import com.telebionica.sql.predicates.Predicate;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.join.Join;
import com.telebionica.sql.power.PowerManager;
import com.telebionica.sql.setu.SetForUpdate;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aldo
 */
public class Query<E> {

    private final PowerManager powerManager;

    private QTYPE qtype;
    private String schema;
    private String rootAlias;
    private Class<E> entityClass;

    private String[] selectFieldNames;
    private String[] deleteAliases;

    private List<Order> orders;

    private Integer firstResult;
    private Integer maxResults;

    private List<Join> joins;
    private List<Fetch> fetchs;
    private List<Predicate> predicates;
    private List<SetForUpdate> sets;

    public Query(PowerManager powerManager) throws PowerQueryException {
        this.powerManager = powerManager;
    }

    public Query schema(String schema) throws PowerQueryException {
        this.schema = schema;
        return this;
    }

    public Query<E> select(String... columnNames) throws PowerQueryException {
        qtype = QTYPE.SELECT;
        this.selectFieldNames = columnNames;
        return this;
    }

    public Query<E> from(Class<E> entityClass) throws PowerQueryException {
        return from(entityClass, "e");
    }

    public Query<E> from(Class<E> entityClass, String alias) throws PowerQueryException {

        if (qtype == null) {
            throw new PowerQueryException("No se ha establecido el qtype, use previamente select() o delete(String ...aliases)");
        }

        if (alias == null) {
            throw new PowerQueryException("Alias no puede ser nulo en from(Class entityClass, String alias)");
        }

        this.rootAlias = alias;
        this.entityClass = entityClass;

        this.orders = new ArrayList();
        this.predicates = new ArrayList();
        this.sets = new ArrayList();
        this.joins = new ArrayList();
        this.fetchs = new ArrayList();

        return this;
    }

    public Query<E> join(String fieldPath, String alias) throws PowerQueryException {
        return join(fieldPath, alias, JOINTYPE.INNER);
    }

    public Query<E> left(String field, String alias) throws PowerQueryException {
        return join(field, alias, JOINTYPE.LEFT);
    }

    public Query<E> right(String field, String alias) throws PowerQueryException {
        return join(field, alias, JOINTYPE.RIGHT);
    }

    public Query<E> join(String fieldPath, String alias, JOINTYPE jointype) throws PowerQueryException {

        if (rootAlias == null) {
            throw new PowerQueryException("No se ha establecido un alias del root en en from(Class entityClass, String alias) ");
        }

        if (jointype == null) {
            throw new PowerQueryException("Jointype no puede ser nulo");
        }

        joins.add(new Join(fieldPath, alias, jointype));

        return this;
    }
    
    public Query<E> fetch(Fetch fetch) throws PowerQueryException {
        if (rootAlias == null) {
            throw new PowerQueryException("No se ha establecido un alias del root en en from(Class entityClass, String alias) ");
        }
        fetchs.add(fetch);
        return this;
    }
    
    public Query<E> fetch(String collectionField, String alias) throws PowerQueryException {

        if (rootAlias == null) {
            throw new PowerQueryException("No se ha establecido un alias del root en en from(Class entityClass, String alias) ");
        }

        fetchs.add(new Fetch(collectionField, alias));
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
        // order.setQuery(this);
        orders.add(order);
        return this;
    }

    public Query set(SetForUpdate set) {
        set.setQuery(this);
        sets.add(set);
        return this;
    }

    public Query<E> update(Class<E> entityClass, String alias) throws PowerQueryException {

        if (qtype != null && (qtype == QTYPE.SELECT || qtype == QTYPE.INSERT || qtype == QTYPE.DELETE)) {
            throw new PowerQueryException("Ya se ha fijado el query como " + qtype + " elija oportunamente un tipo");
        }

        qtype = QTYPE.UPDATE;

        this.rootAlias = alias;
        this.entityClass = entityClass;

        this.orders = new ArrayList();
        this.predicates = new ArrayList();
        this.sets = new ArrayList();
        this.joins = new ArrayList();
        return this;
    }

    public Query delete(String... aliases) {
        qtype = QTYPE.DELETE;
        deleteAliases = aliases;
        return this;
    }

    public List<E> list() throws PowerQueryException {
        return powerManager.list(this);
    }

    public Integer count() throws PowerQueryException {
        return powerManager.count(this);
    }
    
    public Integer count(Connection conn) throws PowerQueryException {
        return powerManager.count(this, conn);
    }

    public int execute() throws PowerQueryException {
        return powerManager.execute(this);
    }

    public E unique() throws PowerQueryException{
        return powerManager.unique(this);
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
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

    public QTYPE getQtype() {
        return qtype;
    }

    public void setQtype(QTYPE qtype) {
        this.qtype = qtype;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public void setRootAlias(String rootAlias) {
        this.rootAlias = rootAlias;
    }

    public String[] getSelectFieldNames() {
        return selectFieldNames;
    }

    public void setSelectFieldNames(String[] selectFieldNames) {
        this.selectFieldNames = selectFieldNames;
    }

    public String[] getDeleteAliases() {
        return deleteAliases;
    }

    public void setDeleteAliases(String[] deleteAliases) {
        this.deleteAliases = deleteAliases;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public void setJoins(List<Join> joins) {
        this.joins = joins;
    }

    public List<Fetch> getFetchs() {
        return fetchs;
    }

    public void setFetchs(List<Fetch> fetchs) {
        this.fetchs = fetchs;
    }
    
    public List<Predicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<Predicate> predicates) {
        this.predicates = predicates;
    }

    public List<SetForUpdate> getSets() {
        return sets;
    }

    public void setSets(List<SetForUpdate> sets) {
        this.sets = sets;
    }

    public Class<E> getEntityClass() {
        return entityClass;
    }

    public PowerManager getPowerManager() {
        return powerManager;
    }
    
    
    public static enum QTYPE {
        SELECT, UPDATE, INSERT, DELETE;
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
