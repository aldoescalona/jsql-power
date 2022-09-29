
package com.telebionica.validator.test;

import com.telebionica.risto.batch.model.Proveedor;
import com.telebionica.risto.batch.model.groups.MyForm;
import com.telebionica.sql.test.TestPowerManager;
import com.telebionica.validator.Brocal;
import com.telebionica.validator.Messages;
import com.telebionica.validator.Validator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 *
 * @author aldo
 */
public class ValidateProveedorTest {

    public ValidateProveedorTest() {
    }

    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void test() {

        try {

            TestPowerManager pm = new TestPowerManager();
            pm.setMetadaSchema("RSTX");

            Validator validator = new Validator();
            
            Proveedor proveedor = new Proveedor();
            proveedor.setId(1443625991551L);
            proveedor.setNombre("");
            proveedor.setRfc("CORONA");
            // proveedor.setRazonSocial("Raxon");
            
            List<Messages> list = validator.
                    validate(proveedor, MyForm.class).
                    translate("es");
            
            System.out.println(" Incidencias: " + list);
            
            
             List<Messages> messagesList = pm.validateToUpdate("RST0", proveedor, MyForm.class);
             messagesList = Brocal.translate(messagesList, "es");
             
             System.out.println(" Incidencias: " + messagesList);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
