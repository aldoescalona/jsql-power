/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.risto.batch.model;

import java.math.BigDecimal;

/**
 *
 * @author Aldo
 */
public class Stock {
    
    private Long id;
    private Long sucursalId;
    private Long almacenId;
    private Long productoId;
    private BigDecimal existencia;
    private BigDecimal costo;
    private BigDecimal costoConImpuesto;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSucursalId() {
        return sucursalId;
    }

    public void setSucursalId(Long sucursalId) {
        this.sucursalId = sucursalId;
    }

    public Long getAlmacenId() {
        return almacenId;
    }

    public void setAlmacenId(Long almacenId) {
        this.almacenId = almacenId;
    }

    public Long getProductoId() {
        return productoId;
    }

    public void setProductoId(Long productoId) {
        this.productoId = productoId;
    }

    public BigDecimal getExistencia() {
        return existencia;
    }

    public void setExistencia(BigDecimal existencia) {
        this.existencia = existencia;
    }

    public BigDecimal getCosto() {
        return costo;
    }

    public void setCosto(BigDecimal costo) {
        this.costo = costo;
    }

    public BigDecimal getCostoConImpuesto() {
        return costoConImpuesto;
    }

    public void setCostoConImpuesto(BigDecimal costoConImpuesto) {
        this.costoConImpuesto = costoConImpuesto;
    }
    
}
