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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.ibatis.sequence.CCr;
import org.apache.ibatis.sequence.CSequence;
import org.apache.ibatis.sequence.CSequenceDefault;
import org.apache.ibatis.sequence.CSequenceObserver;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public class CSequenceImpl extends CSequenceDefault implements CSequence,InitializingBean {
	private int retryTimes = 1;
	//内步长 ,默认为1000，取值在1-100000之间 
	private int innerStep = 1000;
	//id生成器的字段名,默认为name
	private String columnName = "name";
	//存值的列的字段名,默认为value
	private String columnValue = "value";
	//使用的表的表名 ，默认为sequence
	private String tableName = "sequence";

	private JdbcTemplate jdbcTemplate;
	private CSequenceObserver sequenceObserver;
	private DataSourceTransactionManager transactionManager;  
	
	private Semaphore semaphore = new Semaphore(1);
	private ExecutorService executorService = Executors.newFixedThreadPool(1);
	  
    public void setTransactionManager(DataSourceTransactionManager transactionManager) {  
        this.transactionManager = transactionManager;  
    }

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setInnerStep(int innerStep) {
		this.innerStep = innerStep;
	}
	
	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void setColumnValue(String columnValue) {
		this.columnValue = columnValue;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		sequenceObserver = new CSequenceObserverImpl(tableName, columnName, columnValue, 
				innerStep,jdbcTemplate, transactionManager, log);
	}
	
	protected int getRetryTimes() {
		return this.retryTimes;
	}
	
	protected void notifyFindAndUpdate() {
		if(semaphore.tryAcquire()) {
			executorService.execute(new Runnable(){
				public void run() {
					CCr ccr = sequenceObserver.findAndUpdate();
					ccid.set(ccr.id() <=0 ? 1 : ccr.id() + 1);
					curr.set(innerStep);
					synchronized (ccid) {
						ccid.notifyAll();
					}
					semaphore.release();
				}
			});
		}
	}
}
