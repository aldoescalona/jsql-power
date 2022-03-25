/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telebionica.sql.dialect;

import com.telebionica.sql.data.PowerColumnType;
import com.telebionica.sql.order.Order;
import com.telebionica.sql.power.AbstractPowerManager;
import com.telebionica.sql.predicates.Predicate;
import com.telebionica.sql.query.JoinNode;
import com.telebionica.sql.query.ParametrizedQuery;
import com.telebionica.sql.query.Query;
import com.telebionica.sql.query.QueryBuilderException;
import com.telebionica.sql.setu.SetForUpdate;
import com.telebionica.sql.type.TableType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author aldo
 */
public class MySQLDialect extends AbstractDialect {

    public MySQLDialect(AbstractPowerManager pm) {
        super(pm);
    }
    
    @Override
    public ParametrizedQuery dryRun(Query query, boolean count, Connection conn) throws SQLException, QueryBuilderException {
        
        List<PowerColumnType> params = new ArrayList();
        List<PowerColumnType> selectColumns = null;

        TableType tableType = pm.getTableType(query.getEntityClass(), conn);
        
        if (tableType == null) {
            throw new QueryBuilderException("No esta definida la tabla en donde construir el query");
        }
        
        List<JoinNode> rootJoinNodes  = pm.toJoinNodes(tableType, query.getJoins(), conn);

        
        /* Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.PROTECTED).setPrettyPrinting().create();
        String json = gson.toJson(rootJoinNodes);
        System.out.println(" JOINS: " + json); */

        String queryString = "";

        StringBuilder sb = new StringBuilder();
        String joiners = this.joins(query.getRootAlias(), rootJoinNodes, query);

        if (query.getQtype() == Query.QTYPE.SELECT) {

            sb.append("SELECT ");
            if (!count) {

                selectColumns = selectIndexator(query, tableType);

                Iterator<PowerColumnType> selectIt = selectColumns.iterator();
                while (selectIt.hasNext()) {
                    PowerColumnType selColumnType = selectIt.next();

                    sb.append(query.getRootAlias()).append(".");
                    sb.append(selColumnType.getColumnType().getColumnName());
                    sb.append(" as ").append(selColumnType.getColumnAlias());

                    if (selectIt.hasNext()) {
                        sb.append(", ");
                    }
                }

                sb.append(selectJoins(rootJoinNodes));
            } else {
                sb.append("COUNT(*)");
            }

            sb.append("\nFROM ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");
            sb.append(joiners);

            StringBuilder wheresb = new StringBuilder();
            if (query.getPredicates().size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> it = query.getPredicates().iterator();
                while (it.hasNext()) {
                    Predicate p = it.next();
                    p.build(rootJoinNodes, conn);

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (it.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb);

            if (count) {
                queryString = sb.toString();
            } else {
                if (query.getOrders().size() > 0) {
                    sb.append("\nORDER BY ");
                    Iterator<Order> it = query.getOrders().iterator();
                    while (it.hasNext()) {
                        Order order = it.next();
                        order.build(rootJoinNodes, conn);
                        sb.append(order.getOrderStatement());

                        if (it.hasNext()) {
                            sb.append(", ");
                        }
                    }
                }

                queryString = sb.toString();
                queryString = limit(queryString, query);
            }

        } else if (query.getQtype() == Query.QTYPE.DELETE) {

            sb.append("DELETE ");

            List<String> deletes = new ArrayList();
            if (query.getDeleteAliases() == null || query.getDeleteAliases().length == 0) {
                deletes.add(query.getRootAlias());
            } else {
                deletes.addAll(Arrays.asList(query.getDeleteAliases()));
            }

            Iterator<String> delIt = deletes.iterator();

            while (delIt.hasNext()) {
                String del = delIt.next();
                sb.append(del);
                if (delIt.hasNext()) {
                    sb.append(",");
                }
            }

            sb.append("\nFROM ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");
            sb.append(joiners);

            StringBuilder wheresb = new StringBuilder();
            if (query.getPredicates().size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> it = query.getPredicates().iterator();
                while (it.hasNext()) {
                    Predicate p = it.next();
                    p.build(rootJoinNodes, conn);

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (it.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb);

            queryString = sb.toString();
        } else if (query.getQtype() == Query.QTYPE.UPDATE) {
            sb.append("UPDATE ");
            if (query.getSchema() != null) {
                sb.append(query.getSchema()).append(".");
            }
            sb.append(tableType.getName()).append(" ").append(query.getRootAlias()).append(" ");

            sb.append(joiners);

            if (!query.getSets().isEmpty()) {
                sb.append("\nSET ");
            }

            Iterator<SetForUpdate> it = query.getSets().iterator();
            while (it.hasNext()) {
                SetForUpdate p = it.next();
                p.build(rootJoinNodes, conn);

                sb.append(p.getAsignStatement());
                if (p.hasValue()) {
                    params.add(p.getValueType());
                }

                if (it.hasNext()) {
                    sb.append(", ");
                }
            }

            StringBuilder wheresb = new StringBuilder();
            if (query.getPredicates().size() > 0) {
                wheresb.append("\nWHERE ");

                Iterator<Predicate> itp = query.getPredicates().iterator();
                while (itp.hasNext()) {
                    Predicate p = itp.next();
                    p.build(rootJoinNodes, conn);

                    wheresb.append(p.getPredicateStatement());
                    params.addAll(p.getValueTypes());

                    if (itp.hasNext()) {
                        wheresb.append(" AND ");
                    }
                }
            }
            sb.append(wheresb.toString());
            queryString = sb.toString();
        }

        System.out.println(" QUERY: " + queryString);

        ParametrizedQuery pq = new ParametrizedQuery(queryString, params, selectColumns, rootJoinNodes);
        return pq;
    }    
    
    
}
