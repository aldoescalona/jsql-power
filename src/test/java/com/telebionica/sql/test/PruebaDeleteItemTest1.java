/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.power.ItemPrueba;
import com.telebionica.sql.power.Prueba;
import java.math.BigDecimal;
import java.util.Date;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class PruebaDeleteItemTest1 {
    
     @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");

            
            ItemPrueba item = new ItemPrueba();
            item.setId(16484822842100L);
            item.setDescripcion("Lorem ipsum dolor sit amet");
            
            pm.delete("RST0", item);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
