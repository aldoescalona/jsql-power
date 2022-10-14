/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.telebionica.sql.test.embededid;

import com.mycompany.maker.Cliente;
import com.mycompany.maker.ClientePK;
import com.telebionica.sql.test.TestPowerManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class EmbeddedDeleteTest {

    @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("powertest");
            pm.setDebugConfig(true);

            ClientePK id = new ClientePK(4, 1);
            Cliente cte = new Cliente();
            cte.setClientePK(id);
            cte.setNombre("Aldo Escalona");
            cte.setFecha("HOY");
            

            pm.delete("powertest", cte);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
