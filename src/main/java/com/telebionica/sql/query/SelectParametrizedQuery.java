/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

import java.util.List;

/**
 *
 * @author aldo
 */
public class SelectParametrizedQuery<T> extends ParametrizedQuery {

    private Class<T> targetClass;
    private List<CollectionParametrizedQuery> fetchs;

    public SelectParametrizedQuery(Class targetClass) {
        this.targetClass = targetClass;
    }

    public Class<T> getTargetClass() {
        return targetClass;
    }

    public void setTargetClass(Class<T> targetClass) {
        this.targetClass = targetClass;
    }
    
    
    
    public List<CollectionParametrizedQuery> getFetchs() {
        return fetchs;
    }

    public void setFetchs(List<CollectionParametrizedQuery> fetchs) {
        this.fetchs = fetchs;
    }

    public CollectionParametrizedQuery getFetchParametrizedQuery(String id) {
        CollectionParametrizedQuery ct = fetchs.stream().filter(e -> e.getCollectionFieldName().equals(id)).findAny().orElse(null);
        return ct;
    }
}
