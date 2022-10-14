package com.telebionica.sql.test.embededid;

import com.mycompany.maker.Cliente;
import com.telebionica.sql.test.query.*;
import com.telebionica.risto.batch.model.Factura;
import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.test.TestPowerManager;
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
public class EmbeddedSelectTest {

    
    @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("powertest");
            pm.setDebugConfig(true);
            
            Query query = pm.createQuery();

            query.schema("powertest").
                    select().
                    from(Cliente.class, "c").
                    // join("pruebaId", "p").
                    where(Predicates.eq("c.cteId", 1));
            
            List<Factura> list = query.list();
            
            System.out.println(" LIST: " + list);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
