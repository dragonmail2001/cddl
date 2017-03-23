/*
 * Copyright 2015-2115 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @email   dragonmail2001@163.com
 * @author  jinglong.zhaijl
 * @date    2015-10-24
 *
 */
package org.apache.ibatis.sequence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.sequence.CCr;
import org.apache.ibatis.sequence.CId;
import org.apache.ibatis.sequence.CSequenceObserver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class CSequenceObserverImpl implements CSequenceObserver {
	private Log log = null;
	private int innerStep = 1000;
	
	//sql 
	private String update = null;
	private String select  = null;

	private JdbcTemplate jdbcTemplate = null;
	private DataSourceTransactionManager transactionManager = null;  

	public CSequenceObserverImpl(String tableName, String columnName, String columnValue, 
			int innerStep, JdbcTemplate jdbcTemplate, DataSourceTransactionManager transactionManager, Log log) {
		this.log = log;
		this.innerStep = innerStep;
		this.jdbcTemplate = jdbcTemplate;
		this.transactionManager = transactionManager;
		
		StringBuffer sb = new StringBuffer("select id, version from ");
		sb.append(tableName).append(" where ");
		sb.append(columnName).append("='");
		sb.append(columnValue).append("'");
		this.select = sb.toString();
		
		sb = new StringBuffer("update ");
		sb.append(tableName).append(" ");
		sb.append("set id=id+?,version=version+1 where ");
		sb.append(columnName).append("='").append(columnValue);
		sb.append("' and version=?");
		this.update = sb.toString();
	}
	
	public CCr findAndUpdate() {
		return new TransactionTemplate(transactionManager).execute(new TransactionCallback<CCr>(){
			@Override
			public CCr doInTransaction(TransactionStatus ts) {
				try {
	            	final CId cid = jdbcTemplate.queryForObject(select, new RowMapper<CId>(){
						public CId mapRow(ResultSet rs, int num)
								throws SQLException {
							CId cid = new CId();
							cid.setId(rs.getLong("id")); 
							cid.setVersion(rs.getLong("version")); 
							return cid;
						} 
	            	});
	            	
	            	if(cid == null) {
	            		return null;
	            	}
	            	
	                int icnt = jdbcTemplate.update(update, new PreparedStatementSetter(){
						public void setValues(PreparedStatement ps)
								throws SQLException {
							 ps.setLong(1, innerStep);  
		                     ps.setLong(2, cid.getVersion());  
						}
	                });
	                
	                if(icnt > 0 ) {
	                	return new CCr(cid.getId() <= 0 ? 0L : cid.getId(), innerStep);
	                }
				}catch(Exception exc) {
					log.error(exc.getMessage(), exc);
					ts.setRollbackOnly();
				}
				
                return null ;
			}   
        });  
	}

}
