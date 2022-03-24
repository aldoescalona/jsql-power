/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class PruebaUpdateTest {
    
     @Test
    public void test() {

        try {

            PowerManager pm = new PowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();

            List<Prueba> list = query.schema("RST0").select().from(Prueba.class)
                    .where(Predicates.eq("id", 1L))
                    .list();
            
            System.out.println(" LIST: " + list);
            
            Prueba p = list.get(0);
            p.setDatoIntA(120);
            
            pm.update("RST0", p);
            pm.update("RST0", p, "datoIntA");
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
