package com.telebionica.sql.test.query;

import com.telebionica.risto.batch.model.Producto;
import com.telebionica.sql.predicates.Like;
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
public class LikeProductoSelectTest {

    @Test
    public void producto() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();

            query.schema("RST0").
                    select().
                    from(Producto.class, "e").
                    //where(Predicates.rawPredicate("e.nombre like '%Coca%'"));
                    where(Predicates.like("nombre", "Coca", Like.MATCH_MODE.START));

            List<Producto> list = query.list();
            

            System.out.println(" LIST: " + list);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }


}
