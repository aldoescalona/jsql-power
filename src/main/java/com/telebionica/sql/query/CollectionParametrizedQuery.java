/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.query;

/**
 *
 * @author aldo
 */
public class CollectionParametrizedQuery extends ParametrizedQuery {

    private String collectionFieldName;
    private Class targetClass;

    public CollectionParametrizedQuery(String collectionFieldName, Class targetClass) {
        this.collectionFieldName = collectionFieldName;
        this.targetClass = targetClass;
    }

    public String getCollectionFieldName() {
        return collectionFieldName;
    }

    public void setCollectionFieldName(String collectionFieldName) {
        this.collectionFieldName = collectionFieldName;
    }

    public Class getTargetClass() {
        return targetClass;
    }

    public void setTargetClass(Class targetClass) {
        this.targetClass = targetClass;
    }

    

}
