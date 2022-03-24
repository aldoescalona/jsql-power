package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.predicates.Comparison;
import com.telebionica.sql.predicates.Junction;
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
public class FacturaCountTest {

    public FacturaCountTest() {
    }

    @org.junit.jupiter.api.BeforeAll
    public static void setUpClass() throws Exception {
    }

    @org.junit.jupiter.api.AfterAll
    public static void tearDownClass() throws Exception {
    }

    @org.junit.jupiter.api.BeforeEach
    public void setUp() throws Exception {
    }

    @org.junit.jupiter.api.AfterEach
    public void tearDown() throws Exception {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void prueba() {

        System.out.println(" PROBANDO PROBANDO");

        /*Field[] fields = Types.class.getDeclaredFields();
        
        for(Field f:fields){
            System.out.println(" " + f.getName());
        }*/
        try {

            PowerManager pm = new PowerManager();
            pm.setMetadaSchema("RSTX");
            Query query = pm.createQuery();


            query.schema("RST0").
                    select().
                    from(Factura.class, "f").
                    join("emisorFactura", "ef1").
                    where(Predicates.eq("ef1.id", 1476333018261L)).
                    addOrder(Order.asc("f.folioSat")).
                    addOrder(Order.desc("f.estado")).
                    setFirstResult(0).
                    setMaxResults(5);

            int c = query.count();
            
            System.out.println(" COUNT: " + c);
            
           
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
