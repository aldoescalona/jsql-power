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
public class QueryBuilderException extends Exception{

    public QueryBuilderException() {
    }

    public QueryBuilderException(String message) {
        super(message);
    }

    public QueryBuilderException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryBuilderException(Throwable cause) {
        super(cause);
    }

    public QueryBuilderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
    
    
}
