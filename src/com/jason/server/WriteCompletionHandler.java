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
public class WriteCompletionHandler implements CompletionHandler<Integer, Response>{
	private static Log log = LogFactory.getLog(WriteCompletionHandler.class);// 日志记录器
	public void cancelled(Response session) {  
	 log.warn("Session write operation was canceled");  
	}  
	
	
	public void completed(Integer result, Response session) {
	 log.debug("Session write +" + result + " bytes");
	 
	 // 当前buffer 没有传输完毕，继续传输
	 if(session.getCurrentBuffer().position() + result < session.getCurrentBuffer().limit()){
		 session.getCurrentBuffer().position(session.getCurrentBuffer().position() + result);		 
	 }else{
		 session.getCurrentBuffer().clear();
		 BufferPool.getInstance().releaseBuffer(session.getCurrentBuffer());// 将用完的缓冲区放回缓冲区池
		 session.setCurrentBuffer(null);
	 }
	 session.write(session.getClient());
	}  
	
	public void failed(Throwable exc, Response session) {  
		log.error("Session read error", exc);
		exc.printStackTrace();
		session.getClient().close();  
	}
}
