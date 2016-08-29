package com.ov.spark.training;

import java.util.Date;

public class ParseData {

	String mIp;
	Date mTimesTamp;
	String mUrl;
	String mRequete;
	public ParseData(String iIp, Date iTimesTamp, String iUrl, String iRequete) {
		this.mIp = iIp;
		this.mTimesTamp = iTimesTamp;
		this.mRequete = iRequete;
		this.mUrl = iUrl;		
		
	}
	public String getmIp() {
		return mIp;
	}
	public void setmIp(String mIp) {
		this.mIp = mIp;
	}
	public Date getmTimesTamp() {
		return mTimesTamp;
	}
	public void setmTimesTamp(Date mTimesTamp) {
		this.mTimesTamp = mTimesTamp;
	}
	public String getmRequete() {
		return mRequete;
	}
	public void setmRequete(String mRequete) {
		this.mRequete = mRequete;
	}
	public String getmUrl() {
		return mUrl;
	}
	public void setmUrl(String mUrl) {
		this.mUrl = mUrl;
	}



}
