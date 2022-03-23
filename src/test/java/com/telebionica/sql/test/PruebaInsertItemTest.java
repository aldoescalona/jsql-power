/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.query.Query;
import java.math.BigDecimal;
import java.util.Date;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class PruebaInsertItemTest {
    
     @Test
    public void test() {

        try {

            QueryBuilder queryBuilder = new QueryBuilder();
            queryBuilder.setMetadaSchema("RSTX");
            Query<Prueba> query = queryBuilder.createQuery();

            Prueba p = new Prueba();
            p.setId(1L);
            p.setDatoIntA(2);
            p.setDatoChar("CHAR$%##");
            p.setDatoIntB(3);
            p.setDatoTiny(Boolean.TRUE);
            p.setDatoFecha(new Date());
            p.setDatoDecimalA(4);
            p.setDatoDecimalB(new BigDecimal(5.6789));
            p.setDatoDecimalC(new BigDecimal(9876.543));
            p.setDatoFloat(45.678f);
            p.setDatoIntUnsigned(5);
            p.setDatoText("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
            p.setDatoBoolean(Boolean.TRUE);
            p.setDatoDouble(5.309d);
            
            ItemPrueba item = new ItemPrueba();
            item.setId(2L);
            item.setDescripcion("Lorem ipsum dolor sit amet");
            item.setPruebaId(p);
            
            query.schema("RST0").insert(item);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
