/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.telebionica.sql.test.embededid;

import com.mycompany.maker.Cliente;
import com.mycompany.maker.ClientePK;
import com.mycompany.maker.Domiclio;
import com.telebionica.sql.test.TestPowerManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class EmbeddedInsertTest {

    @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("powertest");
            pm.setDebugConfig(true);

            ClientePK id = new ClientePK(5, 1);
            Cliente cte = new Cliente();
            cte.setClientePK(id);
            cte.setNombre("Aldo");
            cte.setFecha("HOY");
            

            pm.insert("powertest", cte);
            
            Domiclio dom = new Domiclio();
            dom.setClienteId(cte);
            dom.setDomiclioId(12);
            dom.setCalle("calle");
            dom.setCiudad("ciudad");
            
            pm.insert("powertest", dom);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String hello = null;
        Assertions.assertNull(hello);
    }
}
