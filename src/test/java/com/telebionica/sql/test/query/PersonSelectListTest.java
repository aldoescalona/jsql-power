package com.telebionica.sql.test.query;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.risto.batch.model.Person;
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
public class PersonSelectListTest {

    
    @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RST0");
            Query query = pm.createQuery();


            query.schema("RST0").
                    select().
                    from(Person.class, "e").
                    join("personDetail", "pd");
            
            List<Factura> list = query.list();
            
            System.out.println(" LIST: " + list);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
