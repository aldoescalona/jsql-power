/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test.update;

import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.setu.SetsForUpdate;
import com.telebionica.sql.test.TestPowerManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class PruebaUpdateQueryTest {
    
     @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            Query<Prueba> query = pm.createQuery();

            /*int c = query.schema("RST0").update(Prueba.class, "e")
                    .set(SetsForUpdate.value("e.datoIntA", 89))
                    .where(Predicates.eq("e.id", 1))
                    .execute();*/
            
            int c = query.schema("RST0").update(ItemPrueba.class, "e")
                    .join("pruebaId", "p")
                    .set(SetsForUpdate.value("e.descripcion", "prueba update join 2"))
                    .set(SetsForUpdate.value("p.datoIntA", 87))
                    .where(Predicates.eq("p.id", 1))
                    .execute();
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
