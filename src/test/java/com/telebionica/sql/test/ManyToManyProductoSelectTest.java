package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Producto;
import com.telebionica.risto.batch.model.Proveedor;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Fetch;
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
public class ManyToManyProductoSelectTest {

    @Test
    public void producto() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();

            query.schema("RST0").
                    select().
                    from(Producto.class, "e").
                    fetch("proveedorList", "pv").
                    where(Predicates.rawPredicate("e.nombre like '%Coca-Cola mediana Caja 24pzs%'"));

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

            Fetch<Producto> fecth = new Fetch("productoList", "pd");
            fecth.addOrder(Order.asc("pd.nombre"));

            query.schema("RST0").
                    select().
                    from(Proveedor.class, "e").
                    // fetch("productoList", "pd").
                    fetch(fecth).
                    where(Predicates.rawPredicate("e.nombre like '%coca%'"));

            List<Producto> list = query.list();

            System.out.println(" LIST: " + list);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }

}
