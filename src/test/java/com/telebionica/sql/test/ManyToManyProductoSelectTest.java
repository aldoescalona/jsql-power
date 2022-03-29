package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.risto.batch.model.Producto;
import com.telebionica.risto.batch.model.Proveedor;
import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
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
public class ManyToManyProductoSelectTest {

    
    @Test
    // @Disabled
    public void producto() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();


            query.schema("RST0").
                    select().
                    from(Producto.class, "e").
                    where(Predicates.rawPredicate("e.nombre like '%pollo%'"));
            
            List<Producto> list = query.list();
            
            System.out.println(" LIST: " + list);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
    
    @Test
    public void proveedor() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();


            query.schema("RST0").
                    select().
                    from(Proveedor.class, "e").
                    where(Predicates.rawPredicate("e.nombre like '%pollo%'"));
            
            List<Producto> list = query.list();
            
            System.out.println(" LIST: " + list);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
