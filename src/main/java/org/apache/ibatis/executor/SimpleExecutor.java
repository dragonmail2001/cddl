/**
 *    Copyright 2009-2015 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import org.apache.ibatis.annotations.Sdt;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.ds.CDatasourceHolder;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import com.googlecode.aviator.AviatorEvaluator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class SimpleExecutor extends BaseExecutor {

  public SimpleExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.update(stmt);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.<E>query(stmt, resultHandler);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    return handler.<E>queryCursor(stmt);
  }

  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
    return Collections.emptyList();
  }

  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
	String sql = handler.getBoundSql().getSql(), tbl = null, fld = null;
	if(sql.startsWith(":")) {
		String[] lines = sql.split(":");
		if(lines.length != 4) {
			throw new SQLException(new StringBuilder("seperator table sql-err:").append(sql).toString());
		}
		
		sql = lines[3].trim();
		tbl = lines[1].trim();
		fld = lines[2].trim();
		
		handler.getBoundSql().setSql(sql);
		
		String[] flds = fld.split(",");
	    List<ParameterMapping> parameterMappings = handler.getBoundSql().getParameterMappings();
	    if (parameterMappings == null) {
	    	throw new SQLException(new StringBuilder("seperator table field-err:").append(sql).toString());
	    }	
	    
	    HashMap<String, Object> map = new HashMap<String, Object>();
		for(String field: flds) {
		  Object value = null;
		  if (handler.getBoundSql().hasAdditionalParameter(field)) { // issue #448 ask first for additional params
            value = handler.getBoundSql().getAdditionalParameter(field);
          } else if (handler.getParameterHandler() != null && handler.getParameterHandler().getParameterObject() == null) {
            value = null;
          } else if (handler.getParameterHandler() != null && configuration.getTypeHandlerRegistry().
        		  hasTypeHandler(handler.getParameterHandler().getParameterObject().getClass())) {
        	for(ParameterMapping pm : handler.getBoundSql().getParameterMappings()) {
        	  if(field.equals(pm.getResultMapId()) || field.equals(pm.getProperty())) {
        	    value = handler.getParameterHandler().getParameterObject();
        	    break;
        	  }
        	}
          } else {
            MetaObject metaObject = configuration.newMetaObject(handler.getParameterHandler().getParameterObject());
            try { value = metaObject.getValue(field); }catch(BindingException bexc) {}
          }
		  
		  if(value == null) {
			  throw new SQLException(new StringBuilder("seperator table value-null-err(resultmapdef??):").append(field).toString());
		  }
		  
		  map.put(field, value);
		}
		
		Long tidx = null, didx = null;
		String[] tbls = tbl.split(",");
		for(String table : tbls) {
			Sdt sdt = configuration.getSdt(table.trim());
			if(sdt == null) {
				throw new SQLException(new StringBuilder("seperator table config-err:").append(table.trim()).toString());
			}
			
			Long cidx = 0L, citx = 0L;
			try {
				citx = (Long)AviatorEvaluator.execute(sdt.exp(), map);
				citx = citx % sdt.tbc();
			} catch(Exception exc) {
				throw new SQLException(new StringBuilder("seperator table exp-err:").append(table.trim()).toString());
			}
			
			cidx = citx / sdt.dbc();
			if(tidx == null || didx == null) {
				tidx = citx;
				didx = cidx;
			}else{
				if(didx != cidx) {
					throw new SQLException(new StringBuilder("seperator table didx-err:").append(table.trim()).toString());
				}
			}
			
			String target = String.format("%s_%d", table, tidx);
			String regex = String.format("@%s@", table);
			sql = sql.replaceAll(regex, target);
			handler.getBoundSql().setSql(sql);
			
			CDatasourceHolder.instance().setRoutingIndex(didx.intValue());
		}
		
	}
	
    Statement stmt;
    Connection connection = getConnection(statementLog);
    stmt = handler.prepare(connection);
    handler.parameterize(stmt);
    return stmt;
  }

}
