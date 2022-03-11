/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.risto.batch.model;

/**
 *
 * @author Aldo
 */
public class CicloTicket {
    
    private Long id;
    private Ticket ticket;
    private Long cierreId;
    private Boolean replicado;
    private Boolean unificado;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Ticket getTicket() {
        return ticket;
    }

    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }

    public Long getCierreId() {
        return cierreId;
    }

    public void setCierreId(Long cierreId) {
        this.cierreId = cierreId;
    }

    public Boolean getReplicado() {
        return replicado;
    }

    public void setReplicado(Boolean replicado) {
        this.replicado = replicado;
    }

    public Boolean getUnificado() {
        return unificado;
    }

    public void setUnificado(Boolean unificado) {
        this.unificado = unificado;
    }
    
}
