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
public class Fusion {

    private Long id;
    private Long fusionPadreId;
    private Long ticketId;
    private Long itemTicketId;
    private Long productoId;
    private Long almacenId;
    private BigDecimal cantidadSinMerma;
    private BigDecimal cantidad;
    private BigDecimal costo;
    private Integer tipo;
    private Boolean salida;
    private Integer direccion;
    private Integer estado = 0;
    private Long sucursalId;
    private Long cierreId;
    

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getFusionPadreId() {
        return fusionPadreId;
    }

    public void setFusionPadreId(Long fusionPadreId) {
        this.fusionPadreId = fusionPadreId;
    }

    public Long getTicketId() {
        return ticketId;
    }

    public void setTicketId(Long ticketId) {
        this.ticketId = ticketId;
    }

    public Long getItemTicketId() {
        return itemTicketId;
    }

    public void setItemTicketId(Long itemTicketId) {
        this.itemTicketId = itemTicketId;
    }

    public Long getProductoId() {
        return productoId;
    }

    public void setProductoId(Long productoId) {
        this.productoId = productoId;
    }

    public Long getAlmacenId() {
        return almacenId;
    }

    public void setAlmacenId(Long almacenId) {
        this.almacenId = almacenId;
    }

    public BigDecimal getCantidadSinMerma() {
        return cantidadSinMerma;
    }

    public void setCantidadSinMerma(BigDecimal cantidadSinMerma) {
        this.cantidadSinMerma = cantidadSinMerma;
    }

    public BigDecimal getCantidad() {
        return cantidad;
    }

    public void setCantidad(BigDecimal cantidad) {
        this.cantidad = cantidad;
    }

    public BigDecimal getCosto() {
        return costo;
    }

    public void setCosto(BigDecimal costo) {
        this.costo = costo;
    }

    public Integer getTipo() {
        return tipo;
    }

    public void setTipo(Integer tipo) {
        this.tipo = tipo;
    }

    public Boolean getSalida() {
        return salida;
    }

    public void setSalida(Boolean salida) {
        this.salida = salida;
    }

    public Integer getDireccion() {
        return direccion;
    }

    public void setDireccion(Integer direccion) {
        this.direccion = direccion;
    }

    public Integer getEstado() {
        return estado;
    }

    public void setEstado(Integer estado) {
        this.estado = estado;
    }

    public Long getSucursalId() {
        return sucursalId;
    }

    public void setSucursalId(Long sucursalId) {
        this.sucursalId = sucursalId;
    }

    public Long getCierreId() {
        return cierreId;
    }

    public void setCierreId(Long cierreId) {
        this.cierreId = cierreId;
    }
    
}
