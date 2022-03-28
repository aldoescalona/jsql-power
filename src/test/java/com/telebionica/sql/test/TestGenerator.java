/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.test;

import com.telebionica.sql.util.Generator;
import java.io.Serializable;
import java.util.Calendar;

/**
 *
 * @author aldo
 */
public class TestGenerator implements Generator{

    public static long anterior = 0L;
    public static long ctrl = 0L;
    
    @Override
    public Serializable next(String schema, String name) {
        long nuevo = Calendar.getInstance().getTimeInMillis();
        nuevo = nuevo * 10;
        if (ctrl >= 9900) {
            ctrl = 0;
        }
        nuevo += ctrl++;
        anterior = nuevo;
        return nuevo;
        
    }
    
}
