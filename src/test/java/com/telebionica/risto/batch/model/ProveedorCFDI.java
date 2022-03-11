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
@Table(name = "proveedorcfdi")
public class ProveedorCFDI {
    @Id
    @Column(name = "id")
    private Long id;
    @Column(name = "nombre")
    private String nombre;
    private String endpoint;
    private String endpointCancel;
    private String usuario;
    private String contrasena;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpointCancel() {
        return endpointCancel;
    }

    public void setEndpointCancel(String endpointCancel) {
        this.endpointCancel = endpointCancel;
    }

    public String getUsuario() {
        return usuario;
    }

    public void setUsuario(String usuario) {
        this.usuario = usuario;
    }

    public String getContrasena() {
        return contrasena;
    }

    public void setContrasena(String contrasena) {
        this.contrasena = contrasena;
    }
    
    
}
