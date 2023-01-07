/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.maker;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author aldo
 */
@Entity
@Table(name = "domiclio")
@NamedQueries({
    @NamedQuery(name = "Domiclio.findAll", query = "SELECT d FROM Domiclio d"),
    @NamedQuery(name = "Domiclio.findByDomiclioId", query = "SELECT d FROM Domiclio d WHERE d.domiclioId = :domiclioId"),
    @NamedQuery(name = "Domiclio.findByCalle", query = "SELECT d FROM Domiclio d WHERE d.calle = :calle"),
    @NamedQuery(name = "Domiclio.findByCiudad", query = "SELECT d FROM Domiclio d WHERE d.ciudad = :ciudad")})
public class Domiclio implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @Basic(optional = false)
    @NotNull
    @Column(name = "domiclioId")
    private Integer domiclioId;
    @Size(max = 45)
    @Column(name = "calle")
    private String calle;
    @Size(max = 45)
    @Column(name = "ciudad")
    private String ciudad;
    @JoinColumn(name = "clienteId", referencedColumnName = "clienteId")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private Cliente clienteId;

    public Domiclio() {
    }

    public Domiclio(Integer domiclioId) {
        this.domiclioId = domiclioId;
    }

    public Integer getDomiclioId() {
        return domiclioId;
    }

    public void setDomiclioId(Integer domiclioId) {
        this.domiclioId = domiclioId;
    }

    public String getCalle() {
        return calle;
    }

    public void setCalle(String calle) {
        this.calle = calle;
    }

    public String getCiudad() {
        return ciudad;
    }

    public void setCiudad(String ciudad) {
        this.ciudad = ciudad;
    }

    public Cliente getClienteId() {
        return clienteId;
    }

    public void setClienteId(Cliente clienteId) {
        this.clienteId = clienteId;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (domiclioId != null ? domiclioId.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Domiclio)) {
            return false;
        }
        Domiclio other = (Domiclio) object;
        if ((this.domiclioId == null && other.domiclioId != null) || (this.domiclioId != null && !this.domiclioId.equals(other.domiclioId))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.mycompany.maker.Domiclio[ domiclioId=" + domiclioId + " ]";
    }
    
}
