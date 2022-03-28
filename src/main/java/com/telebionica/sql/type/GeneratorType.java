/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.type;

import javax.persistence.GenerationType;

/**
 *
 * @author aldo
 */
public class GeneratorType {
    
    private GenerationType strategy;
    private String name;
    private String generator;
    

    public GeneratorType(GenerationType strategy) {
        this.strategy = strategy;
    }
    
    public GeneratorType(GenerationType strategy, String name, String generator) {
        this.strategy = strategy;
        this.name = name;
        this.generator = generator;
    }

    public GenerationType getStrategy() {
        return strategy;
    }

    public void setStrategy(GenerationType strategy) {
        this.strategy = strategy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGenerator() {
        return generator;
    }

    public void setGenerator(String generator) {
        this.generator = generator;
    }

    

}
