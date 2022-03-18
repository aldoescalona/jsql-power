package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.predicates.Comparison;
import com.telebionica.sql.predicates.Junction;
import com.telebionica.sql.query.Predicates;
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
public class SelectTest {

    public SelectTest() {
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
    public void hello() {

        System.out.println(" PROBANDO PROBANDO");

        try {

            QueryBuilder queryBuilder = new QueryBuilder();
            queryBuilder.setMetadaSchema("RSTX");
            Query<Factura> query = queryBuilder.createQuery();

            Junction or = Predicates.or(Predicates.compare("f.estado", Comparison.COMPARISON_OPERATOR.EQ_SAFENULL, 2), Predicates.isNUll("f.uuid"));

            query.schema("RST0").
                    select().
                    from(Factura.class, "f").
                    left("facturaSustitucion", "fs").
                    left("emisorFactura", "ef1").
                    left("facturaSustitucion.emisorFactura", "ef2").
                    where(Predicates.eq("f.folioSat", "E52BEAA6-D1E3-4BE3-82FD-304A3A12BA37")).
                    // and(or).
                    addOrder(Order.asc("f.folioSat")).
                    addOrder(Order.desc("f.estado")).
                    setFirstResult(0).
                    setMaxResults(5);
            
            List<Factura> list = query.list();
            
            System.out.println(" LIST: " + list);
            
            // query.schema("RST0").from().join("emisorFactura", "ef");
            // query.schema("RST0").from().left("facturaSustitucion", "fs");
            // query.schema("RST0").from().left("facturaSustitucion", "fs").left("emisorFactura", "ef");
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
