/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.risto.batch.model;

import com.telebionica.risto.batch.model.groups.MyForm;
import com.telebionica.risto.batch.model.groups.OtheForm;
import com.telebionica.validator.ann.Unique;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author aldo
 */
@Table(name = "proveedor")
public class Proveedor {
    
    @Id
    @Basic(optional = false)
    @NotNull
    @Column(name = "id")
    private Long id;
    @Basic(optional = false)
    @NotNull(message = "Proveedor.nombre.NotNull", groups = MyForm.class)
    @Size(min = 1, max = 45, message = "Proveedor.nombre.Size", groups = MyForm.class)
    @Column(name = "nombre")
    private String nombre;
    @Basic(optional = false)
    @NotNull(groups = MyForm.class)
    @Size(min = 1, max = 45, groups = MyForm.class)
    @Column(name = "razonSocial")
    private String razonSocial;
    
    @NotNull(groups = OtheForm.class)
    @Size(min = 1, max = 45, groups = OtheForm.class)
    @Unique(message = "Proveedor.rfc.Unique", groups = MyForm.class)
    @Column(name = "rfc")
    private String rfc;

    @JoinTable(name = "producto_proveedor", joinColumns = {
        @JoinColumn(name = "proveedorId", referencedColumnName = "id")}, inverseJoinColumns = {
        @JoinColumn(name = "productoId", referencedColumnName = "id")})
    @ManyToMany(fetch = FetchType.LAZY)
    private List<Producto> productoList;
    
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

    public String getRazonSocial() {
        return razonSocial;
    }

    public void setRazonSocial(String razonSocial) {
        this.razonSocial = razonSocial;
    }

    public String getRfc() {
        return rfc;
    }

    public void setRfc(String rfc) {
        this.rfc = rfc;
    }

    public List<Producto> getProductoList() {
        return productoList;
    }

    public void setProductoList(List<Producto> productoList) {
        this.productoList = productoList;
    }
    
}
