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
public class PowerQueryException extends Exception{

    public PowerQueryException() {
    }

    public PowerQueryException(String message) {
        super(message);
    }

    public PowerQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public PowerQueryException(Throwable cause) {
        super(cause);
    }

    public PowerQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
}
