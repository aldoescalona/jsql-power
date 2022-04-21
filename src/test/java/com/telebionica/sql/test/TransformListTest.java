package com.telebionica.sql.test;

import com.telebionica.risto.batch.model.Factura;
import com.telebionica.sql.power.PowerManager;
import com.telebionica.sql.power.Prueba;
import com.telebionica.sql.power.TRANSFORMTYPE;
import com.telebionica.sql.predicates.Predicates;
import com.telebionica.sql.query.Query;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class TransformListTest {

    @Test
    public void test() {

        try {
            
            int estado = 6;
            Function<PreparedStatement, Void> fun = s -> {
                try {
                    s.setInt(1, estado);
                } catch (SQLException ex) {
                    Logger.getLogger(PowerManager.class.getName()).log(Level.SEVERE, null, ex);
                }
                return null;
            };

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");
            List<Factura> list = pm.transform("select * from RST0.factura where estado = ? limit 10", TRANSFORMTYPE.COLUMNNAME, fun, Factura.class);

            System.out.println(" LIST: " + list);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;

        Assertions.assertNull(hello);
    }
}
