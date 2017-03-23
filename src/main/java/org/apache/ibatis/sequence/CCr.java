package org.apache.ibatis.sequence;

public class CCr {
	private long id;
	private long cr;
	
	public CCr(long id, long cr){
		this.id = id;
		this.cr = cr;
	}
	
	public long id() {
		return id;
	}
	
	public void id(long id) {
		this.id = id;
	}
	
	public long cr() {
		return cr;
	}
	
	public void cr(long cr) {
		this.cr = cr;
	}
}
