/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.maker;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.validation.constraints.NotNull;

/**
 *
 * @author aldo
 */
@Embeddable
public class ClientePK implements Serializable {

    @Basic(optional = false)
    @NotNull
    @Column(name = "clienteId")
    private Integer cteId;
    @Basic(optional = false)
    @NotNull
    @Column(name = "consecutivo")
    private Integer consecutivo;

    public ClientePK() {
    }

    public ClientePK(Integer clienteId, Integer consecutivo) {
        this.cteId = clienteId;
        this.consecutivo = consecutivo;
    }

    public Integer getCteId() {
        return cteId;
    }

    public void setCteId(Integer cteId) {
        this.cteId = cteId;
    }

    public Integer getConsecutivo() {
        return consecutivo;
    }

    public void setConsecutivo(Integer consecutivo) {
        this.consecutivo = consecutivo;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (int) cteId;
        hash += (int) consecutivo;
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof ClientePK)) {
            return false;
        }
        ClientePK other = (ClientePK) object;
        if (this.cteId != other.cteId) {
            return false;
        }
        if (this.consecutivo != other.consecutivo) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.mycompany.maker.ClientePK[ clienteId=" + cteId + ", consecutivo=" + consecutivo + " ]";
    }
    
}
