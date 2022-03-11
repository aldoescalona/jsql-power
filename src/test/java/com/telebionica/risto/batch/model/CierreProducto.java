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
public class CierreProducto {
    
    private Long id;
    private Long cierreId;
    private Long almacenId;
    private Long productoId;
    private BigDecimal salidaVenta;
    private BigDecimal salidaTraspaso;
    private BigDecimal salidaTransferencia;
    private BigDecimal  salidaAjusteManual;
    private BigDecimal entradaCompra;
    private BigDecimal entradaTraspaso;
    private BigDecimal entradaTransferencia;
    private BigDecimal  entradaAjusteManual;
    private BigDecimal existenciaFinal;
    // , existenciaFinal, depuracion
    private BigDecimal costo;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCierreId() {
        return cierreId;
    }

    public void setCierreId(Long cierreId) {
        this.cierreId = cierreId;
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

    public BigDecimal getCosto() {
        return costo;
    }

    public void setCosto(BigDecimal costo) {
        this.costo = costo;
    }

    public BigDecimal getSalidaVenta() {
        return salidaVenta;
    }

    public void setSalidaVenta(BigDecimal salidaVenta) {
        this.salidaVenta = salidaVenta;
    }

    public BigDecimal getSalidaTraspaso() {
        return salidaTraspaso;
    }

    public void setSalidaTraspaso(BigDecimal salidaTraspaso) {
        this.salidaTraspaso = salidaTraspaso;
    }

    public BigDecimal getSalidaTransferencia() {
        return salidaTransferencia;
    }

    public void setSalidaTransferencia(BigDecimal salidaTransferencia) {
        this.salidaTransferencia = salidaTransferencia;
    }

    public BigDecimal getSalidaAjusteManual() {
        return salidaAjusteManual;
    }

    public void setSalidaAjusteManual(BigDecimal salidaAjusteManual) {
        this.salidaAjusteManual = salidaAjusteManual;
    }

    public BigDecimal getEntradaCompra() {
        return entradaCompra;
    }

    public void setEntradaCompra(BigDecimal entradaCompra) {
        this.entradaCompra = entradaCompra;
    }

    public BigDecimal getEntradaTraspaso() {
        return entradaTraspaso;
    }

    public void setEntradaTraspaso(BigDecimal entradaTraspaso) {
        this.entradaTraspaso = entradaTraspaso;
    }

    public BigDecimal getEntradaTransferencia() {
        return entradaTransferencia;
    }

    public void setEntradaTransferencia(BigDecimal entradaTransferencia) {
        this.entradaTransferencia = entradaTransferencia;
    }

    public BigDecimal getEntradaAjusteManual() {
        return entradaAjusteManual;
    }

    public void setEntradaAjusteManual(BigDecimal entradaAjusteManual) {
        this.entradaAjusteManual = entradaAjusteManual;
    }

    public BigDecimal getExistenciaFinal() {
        return existenciaFinal;
    }

    public void setExistenciaFinal(BigDecimal existenciaFinal) {
        this.existenciaFinal = existenciaFinal;
    }
    
    
}
