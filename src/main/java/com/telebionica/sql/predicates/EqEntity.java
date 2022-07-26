/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.predicates;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.PowerQueryException;
import com.telebionica.sql.type.ColumnType;
import com.telebionica.sql.type.JoinColumnsType;
import com.telebionica.sql.type.TableType;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.persistence.JoinColumn;

/**
 *
 * @author aldo
 */
public class EqEntity extends Predicate {

    private String fieldName;
    private String statement;
    
    private Object value;
    private List<PowerColumnType> types;
    private boolean has;

    public EqEntity(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public String getPredicateStatement() {
        return statement;
    }

    @Override
    public boolean hasValues() {
        return true;
    }

    @Override
    public List<PowerColumnType> getValueTypes() {
        return types;
    }

    @Override
    public void build(List<JoinNode> rootJoinNodes, Connection conn) throws PowerQueryException {


        TableType tableType = getQuery().getPowerManager().getTableType(getQuery().getEntityClass(), conn);
        JoinColumnsType jcst = tableType.getJoinColumnsType(fieldName);

        TableType otherTT = getQuery().getPowerManager().getTableType(jcst.getFieldClass(), conn);
        if (otherTT == null) {
            throw new PowerQueryException("No existe tabla relacionada a " + jcst.getFieldClass());
        }
        
        List<PowerColumnType> temptypes = new ArrayList();
        
        List<JoinColumn> jcs = jcst.getJoiners();
        for (JoinColumn jc : jcs) {
            
            boolean reverse = jcst.isReverse();
            String jcName = reverse ? jc.referencedColumnName() : jc.name();
            String referencedColumnName = reverse ? jc.name() : jc.referencedColumnName();

            ColumnType ct = otherTT.getColumnType(referencedColumnName);
            PowerColumnType param = new PowerColumnType(ct);
            param.setColumnAlias(jcName);
            
            if(value != null){
                param.getter(value);
            }
            
            temptypes.add(param);
        }

        types = new ArrayList();
        int c = 0;
        StringBuilder sb = new StringBuilder();
        
        Iterator<PowerColumnType> colIt = temptypes.iterator();
        while (colIt.hasNext()) {
            
            PowerColumnType ct = colIt.next();
            
            if(ct.getValue() == null){
                sb.append(String.format("%s.%s is null", getQuery().getRootAlias(), ct.getColumnAlias()));
            }else{
                sb.append(String.format("%s.%s = ?", getQuery().getRootAlias(), ct.getColumnAlias()));
                types.add(ct);
                c++;
            }
            
            if (colIt.hasNext()) {
                sb.append(" AND ");
            }
        }
        
        has = c > 0;
        statement = sb.toString();
    }

    public Object getValue() {
        return value;
    }

}
