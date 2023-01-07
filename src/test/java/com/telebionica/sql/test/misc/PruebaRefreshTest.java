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
public class PruebaRefreshTest {
    
    @Test
    public void test() {
        
        try {
            
            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            
            Prueba prueba = new Prueba();
            prueba.setId(1L);
            
            pm.refresh("RST0", prueba);
            System.out.println(" P: " + prueba);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        String hello = null;
        Assertions.assertNull(hello);
    }
}
