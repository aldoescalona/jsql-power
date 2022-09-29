package com.telebionica.sql.test.misc;

import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.test.TestPowerManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author aldo
 */
public class PruebaGetNoSchemaTest {
    
    @Test
    public void test() {
        
        try {
            
            TestPowerManager pm = new TestPowerManager();
            
            Prueba prueba = pm.get(2L, Prueba.class);
            System.out.println(" P: " + prueba);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        String hello = null;
        Assertions.assertNull(hello);
    }
}
