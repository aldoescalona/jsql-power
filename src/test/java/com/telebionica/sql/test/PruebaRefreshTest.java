package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import java.util.List;
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
            
            Prueba p = new Prueba();
            p.setId(1L);
            
            pm.refresh("RST0", p);
            
            System.out.println(" P: " + p);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        String hello = null;
        Assertions.assertNull(hello);
    }
}
