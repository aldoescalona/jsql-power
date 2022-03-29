/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.power;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 *
 * @author Aldo
 */
@Entity
@Table(name = "prueba")
@NamedQueries({
    @NamedQuery(name = "Prueba.findAll", query = "SELECT p FROM Prueba p")})
public class Prueba implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy=GenerationType.AUTO,  generator = "generator")
    // @GeneratedValue(strategy=GenerationType.SEQUENCE,  generator = "ticket_id_seq")
    // @GeneratedValue(strategy=GenerationType.IDENTITY,  generator = "com.telebionica.commons.Generator")
    @Basic(optional = false)
    @NotNull
    @Column(name = "id")
    private Long id;
    @Basic(optional = false)
    @NotNull
    @Column(name = "datoIntA")
    private int datoIntA;
    @Size(max = 45)
    @Column(name = "datoChar")
    private String datoChar;
    @Column(name = "datoIntB")
    private Integer datoIntB;
    @Column(name = "datoTiny")
    private Boolean datoTiny;
    @Column(name = "datoFecha")
    @Temporal(TemporalType.TIMESTAMP)
    private Date datoFecha;
    @Column(name = "datoDecimalA")
    private Integer datoDecimalA;
    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "datoDecimalB")
    private BigDecimal datoDecimalB;
    @Column(name = "datoDecimalC")
    private BigDecimal datoDecimalC;
    @Column(name = "datoFloat")
    private Float datoFloat;
    @Column(name = "datoIntUnsigned")
    private Integer datoIntUnsigned;
    @Lob
    @Size(max = 65535)
    @Column(name = "datoText")
    private String datoText;
    @Column(name = "datoBoolean")
    private Boolean datoBoolean;
    @Column(name = "datoDouble")
    private Double datoDouble;
    
    @Column(name = "datoEnum")
    @Enumerated(EnumType.ORDINAL)
    private  PreuebaEnumerated.DATO_ENUM datoEnum;
    
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "pruebaId", fetch = FetchType.EAGER)
    private List<ItemPrueba> itemPruebaList;

    public Prueba() {
        // int ordinal = PreuebaEnumerated.DATO_ENUM.ALIMENTO.ordinal();
    }

    public Prueba(Long id) {
        this.id = id;
    }

    public Prueba(Long id, int datoIntA) {
        this.id = id;
        this.datoIntA = datoIntA;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getDatoIntA() {
        return datoIntA;
    }

    public void setDatoIntA(int datoIntA) {
        this.datoIntA = datoIntA;
    }

    public String getDatoChar() {
        return datoChar;
    }

    public void setDatoChar(String datoChar) {
        this.datoChar = datoChar;
    }

    public Integer getDatoIntB() {
        return datoIntB;
    }

    public void setDatoIntB(Integer datoIntB) {
        this.datoIntB = datoIntB;
    }

    public Boolean getDatoTiny() {
        return datoTiny;
    }

    public void setDatoTiny(Boolean datoTiny) {
        this.datoTiny = datoTiny;
    }

    public Date getDatoFecha() {
        return datoFecha;
    }

    public void setDatoFecha(Date datoFecha) {
        this.datoFecha = datoFecha;
    }

    public Integer getDatoDecimalA() {
        return datoDecimalA;
    }

    public void setDatoDecimalA(Integer datoDecimalA) {
        this.datoDecimalA = datoDecimalA;
    }

    public BigDecimal getDatoDecimalB() {
        return datoDecimalB;
    }

    public void setDatoDecimalB(BigDecimal datoDecimalB) {
        this.datoDecimalB = datoDecimalB;
    }

    public BigDecimal getDatoDecimalC() {
        return datoDecimalC;
    }

    public void setDatoDecimalC(BigDecimal datoDecimalC) {
        this.datoDecimalC = datoDecimalC;
    }

    public Float getDatoFloat() {
        return datoFloat;
    }

    public void setDatoFloat(Float datoFloat) {
        this.datoFloat = datoFloat;
    }

    public Integer getDatoIntUnsigned() {
        return datoIntUnsigned;
    }

    public void setDatoIntUnsigned(Integer datoIntUnsigned) {
        this.datoIntUnsigned = datoIntUnsigned;
    }

    public String getDatoText() {
        return datoText;
    }

    public void setDatoText(String datoText) {
        this.datoText = datoText;
    }

    public Boolean getDatoBoolean() {
        return datoBoolean;
    }

    public void setDatoBoolean(Boolean datoBoolean) {
        this.datoBoolean = datoBoolean;
    }

    public Double getDatoDouble() {
        return datoDouble;
    }

    public void setDatoDouble(Double datoDouble) {
        this.datoDouble = datoDouble;
    }

    public PreuebaEnumerated.DATO_ENUM getDatoEnum() {
        return datoEnum;
    }

    public void setDatoEnum(PreuebaEnumerated.DATO_ENUM datoEnum) {
        this.datoEnum = datoEnum;
    }
    
    public List<ItemPrueba> getItemPruebaList() {
        return itemPruebaList;
    }

    public void setItemPruebaList(List<ItemPrueba> itemPruebaList) {
        this.itemPruebaList = itemPruebaList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (id != null ? id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Prueba)) {
            return false;
        }
        Prueba other = (Prueba) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.telebionica.sql.power.Prueba[ id=" + id + " ]";
    }
    
}
