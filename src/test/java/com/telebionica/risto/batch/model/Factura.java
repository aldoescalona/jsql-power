/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.risto.batch.model;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 *
 * @author Aldo
 */
@Table(name = "factura")
public class Factura {
    @Id
    @Column(name = "id")
    private Long id;
    
    @Column(name = "uuid")
    private String folioSat;
    
    @Column(name = "estado")
    private Integer estado;
    private Integer motivoCancela;
    
    @JoinColumn(name = "emisorFacturaId", referencedColumnName = "id")
    @ManyToOne
    private EmisorFactura emisorFactura;
    
    private Receptor receptor;
    
    @JoinColumn(name = "facturaSustitucionId", referencedColumnName = "id")
    @ManyToOne
    private Factura facturaSustitucion;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFolioSat() {
        return folioSat;
    }

    public void setFolioSat(String folioSat) {
        this.folioSat = folioSat;
    }

    public Integer getEstado() {
        return estado;
    }

    public void setEstado(Integer estado) {
        this.estado = estado;
    }

    public Integer getMotivoCancela() {
        return motivoCancela;
    }

    public void setMotivoCancela(Integer motivoCancela) {
        this.motivoCancela = motivoCancela;
    }

    public EmisorFactura getEmisorFactura() {
        return emisorFactura;
    }

    public void setEmisorFactura(EmisorFactura emisorFactura) {
        this.emisorFactura = emisorFactura;
    }

    public Receptor getReceptor() {
        return receptor;
    }

    public void setReceptor(Receptor receptor) {
        this.receptor = receptor;
    }

    public Factura getFacturaSustitucion() {
        return facturaSustitucion;
    }

    public void setFacturaSustitucion(Factura facturaSustitucion) {
        this.facturaSustitucion = facturaSustitucion;
    }
    

    @Override
    public int hashCode() {
        Integer hash = 0;
        hash += (id != null ? id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Factura)) {
            return false;
        }
        Factura other = (Factura) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.telebionica.risto.bean.Factura[ id=" + id + " ]";
    }
    
}
