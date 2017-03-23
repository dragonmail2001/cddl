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
package org.apache.ibatis.sequence;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

public abstract class CSequenceDefault {
	protected static final Log log = LogFactory.getLog(CSequence.class);
	
	protected AtomicLong curr = new AtomicLong(-1L);
	protected AtomicLong ccid = new AtomicLong(-1L);

	public Long next() {
		Long ret = null;
		int retry = getRetryTimes();  
		while(retry-- > 0) {
			long icur = curr.decrementAndGet();
			if(icur >= 0) {
				ret = icur + ccid.get();
				break;
			}
			
			notifyFindAndUpdate();
			
			synchronized (ccid) {
				try {
					ccid.wait();
				} catch (Exception exc) {
					log.error(exc.getMessage(), exc);
				}
			}
		}
		return ret;
	}
	
	protected abstract void notifyFindAndUpdate();
	protected abstract int getRetryTimes();
}
