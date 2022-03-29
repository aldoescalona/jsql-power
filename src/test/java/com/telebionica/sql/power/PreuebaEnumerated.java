/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.power;

/**
 *
 * @author aldo
 */
public class PreuebaEnumerated {

    public static enum DATO_ENUM {

        ALIMENTO("Alimento", "Alimentos"), BEBIDA("Bebida", "Bebidas"), OTRO("Otro", "Otros");

        private DATO_ENUM(String title, String titlePlural) {
            this.title = title;
            this.titlePlural = titlePlural;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getTitlePlural() {
            return titlePlural;
        }

        public void setTitlePlural(String titlePlural) {
            this.titlePlural = titlePlural;
        }

        @Override
        public String toString() {
            return title;
        }
        private String title;
        private String titlePlural;
    }
}
