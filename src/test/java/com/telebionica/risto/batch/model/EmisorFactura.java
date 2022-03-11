/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.risto.batch.model;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;



/**
 *
 * @author Aldo
 */
@Table(name = "emisorfactura")
public class EmisorFactura {
    
    @Id
    @Column(name = "id")
    private Long id;
    
    @Column(name = "rfc")
    private String rfc;
    
    private String fileCer;
    private String fileKey;
    private String clavePrivada;
    private ProveedorCFDI proveedorCFDI;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getRfc() {
        return rfc;
    }

    public void setRfc(String rfc) {
        this.rfc = rfc;
    }

    public String getFileCer() {
        return fileCer;
    }

    public void setFileCer(String fileCer) {
        this.fileCer = fileCer;
    }

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getClavePrivada() {
        return clavePrivada;
    }

    public void setClavePrivada(String clavePrivada) {
        this.clavePrivada = clavePrivada;
    }

    public ProveedorCFDI getProveedorCFDI() {
        return proveedorCFDI;
    }

    public void setProveedorCFDI(ProveedorCFDI proveedorCFDI) {
        this.proveedorCFDI = proveedorCFDI;
    }
    
    
    
}
