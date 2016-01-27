package org.mybatis.spring.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

public class CJdbcTemplateBean implements FactoryBean<Object>, InitializingBean {

	private Object proxyObj;

	public Object getObject() throws Exception {
		return proxyObj;
	}

	public Class<?> getObjectType() {
		return proxyObj == null ? Object.class : proxyObj.getClass();
	}

	public boolean isSingleton() {
		return true;
	}
	
	public void afterPropertiesSet() throws Exception {
		this.proxyObj = Proxy.newProxyInstance(this.getClass().getClassLoader(),
				new Class[] { Class.forName("org.mybatis.spring.jdbc.CJdbcTemplate") }, new InvocationHandler() {
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						if(method.getName().equals("toString") || method.getName().equals("hashCode") ||
								method.getName().equals("getClass") || method.getName().equals("notify") ||
								method.getName().equals("notifyAll") || method.getName().equals("wait")) {
							throw new java.lang.NoSuchMethodError();
						}
						
						//return CConnectionExecutorImpl.connectionExecutor().execute(proxy, method, args, interfaceUrl, sync);
						return null;
					}
				});		
	}	
}
