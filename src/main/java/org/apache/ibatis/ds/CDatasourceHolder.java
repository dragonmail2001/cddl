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
package org.apache.ibatis.ds;

public class CDatasourceHolder {
	private static final ThreadLocal<Integer> holder = new ThreadLocal<Integer>();
	private static final CDatasourceHolder _this = new CDatasourceHolder(); 
	
	public static CDatasourceHolder instance() {
		return _this;
	}

	public void setRoutingIndex(int index) {
		holder.set(index);
	}

	public Integer getRoutingIndex() {
		return holder.get();
	}
}
