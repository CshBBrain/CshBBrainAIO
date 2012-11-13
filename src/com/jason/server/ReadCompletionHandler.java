/**
 * <li>文件名：ReadCompletionHandler.java
 * <li>说明：
 * <li>创建人： CshBBrain, 技术博客：http://cshbbrain.iteye.com/
 * <li>创建日期：2012-10-30
 * <li>修改人： 
 * <li>修改日期：
 */
package com.jason.server;

import java.nio.channels.CompletionHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <li>类型名称：
 * <li>说明：
 * <li>创建人： CshBBrain, 技术博客：http://cshbbrain.iteye.com/
 * <li>创建日期：2012-10-30
 * <li>修改人： 
 * <li>修改日期：
 */
public class ReadCompletionHandler implements CompletionHandler<Integer, Client>{
	private static Log log = LogFactory.getLog(ReadCompletionHandler.class);// 日志记录器
	private MasterServer server;// 主服务器
	public ReadCompletionHandler(MasterServer server){
		this.server = server;
	}
	public void cancelled(Client session) {  
		log.warn("Session(" + session.getIp() + ") read operation was canceled");  
	}  
		
	public void completed(Integer result, Client session) {  
	 if (log.isDebugEnabled())  
	     log.debug("Session(" + session.getIp() + ") read +" + result + " bytes");  
	 if (result < 0) {// 客户端关闭了连接  
	     session.close();  
	     return;  
	 }
	 
	 if(result == 0){
		 session.setReadCount(session.getReadCount() + 1);
		 session.processBlankRead();// 处理空读的情况
	 }
	 
	 if (result > 0){// 读取到客户端的数据  
	     try {
			this.server.getWorkers().take().processResponse(session);
		} catch (InterruptedException e) {
			log.info(e.getMessage());
		}     
	 }
	}  
	
	public void failed(Throwable exc, Client session) {  
		log.error("Session read error", exc);  
		session.readMonitor();  
	}
}
