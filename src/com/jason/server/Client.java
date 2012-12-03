package com.jason.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jason.Config;
import com.jason.server.hander.CoderHandler;
import com.jason.server.hander.DecoderHandler;
import com.jason.server.hander.ProcessHandler;
import com.jason.util.MyStringUtil;

public class Client{
	private static Log log = LogFactory.getLog(Client.class);// 日志记录器
	public static final String TMP_ROOT = "tmpRoot";// 临时文件目录
	public static final String ZORE_FETCH_COUNT = "zoreFetchCount";// 空读最大次数	
	public static final String tmpRoot = Config.getStr(TMP_ROOT);// 文件保存临时目录
	public static Integer zoreFetchCount = Config.getInt(ZORE_FETCH_COUNT);
		
	private MasterServer sockectServer;// 服务器端socket
	private boolean handShak = false;// 是否已经握手
	private ConcurrentLinkedQueue<Response> responseMsgs;// 发送给客户端的消息
	private ConcurrentLinkedQueue<Response> responseMsgsNotCode;// 发送给客户端的消息	
	private ConcurrentLinkedQueue<String> requestMsgs;// 客户端发送的请求消息
	private ConcurrentLinkedQueue<HashMap<String, String>> bizObjects;// 解码客户端请求所得到的业务对象
	private AtomicBoolean inRead = new AtomicBoolean(false);// 读取通信信号量
	private AtomicBoolean inWrite = new AtomicBoolean(false);// 读取通信信号量	
	private Integer readCount = 0;// 读取次数	
	private boolean preBlank = false;//上次读取为空读
	private boolean readDataFlag = true;//数据读取标识
	private Request requestWithFile = null;// 文件接收器
	
	// 定义编码处理器，业务处理器，解码处理器
	private CoderHandler coderHandler;// 编码处理器
	private DecoderHandler decoderHandler;// 解码处理器
	private ProcessHandler processHandler;// 业务处理器
	private String protocolVersion = "0";//协议版本
	private Object session;//连接会话对象，由开发者自己定义使用
	private Object handShakObject;// 握手处理对象
	private Object index;// 客户端在索引
	private String routeAddress = null;// 远程地址
	private String localAddress = null;// 本地地址
	private AsynchronousSocketChannel socketChannel;// 异步socket连结端
	private ReadCompletionHandler readHandler;// 读取处理器
	private WriteCompletionHandler writeHandler;// 回写处理器	
	private ByteBuffer byteBuffer;// 缓冲区
	private ByteBuffer writeByteBuffer;// 回写缓冲区
	public AtomicBoolean getInWrite() {
		return inWrite;
	}

	public void setInWrite(AtomicBoolean inWrite) {
		this.inWrite = inWrite;
	}
	
	public ConcurrentLinkedQueue<Response> getResponseMsgsNotCode() {
		return responseMsgsNotCode;
	}

	public void setResponseMsgsNotCode(
			ConcurrentLinkedQueue<Response> responseMsgsNotCode) {
		this.responseMsgsNotCode = responseMsgsNotCode;
	}
	
	public AsynchronousSocketChannel getSocketChannel() {
		return socketChannel;
	}

	public void setSocketChannel(AsynchronousSocketChannel socketChannel) {
		this.socketChannel = socketChannel;
	}

	public WriteCompletionHandler getWriteHandler() {
		return writeHandler;
	}

	public void setWriteHandler(WriteCompletionHandler writeHandler) {
		this.writeHandler = writeHandler;
	}
	public ByteBuffer getWriteByteBuffer() {
		return writeByteBuffer;
	}

	public void setWriteByteBuffer(ByteBuffer writeByteBuffer) {
		this.writeByteBuffer = writeByteBuffer;
	}

	public Object getIndex() {
		return index;
	}

	public void setIndex(Object index) {
		this.index = index;
	}

	public  <T> T  getHandShakObject() {
		return (T)handShakObject;
	}

	public void setHandShakObject(Object handShakObject) {
		this.handShakObject = handShakObject;
	}

	private boolean isClient = false;// 是否为连接的客户端，默认为不是
	
	public boolean isClient() {
		return isClient;
	}

	public void setClient(boolean isClient) {
		this.isClient = isClient;
	}

	/**
	 * 
	 * <li>方法名：getSession
	 * <li>@param <T>
	 * <li>@return
	 * <li>返回类型：T
	 * <li>说明：获取连接会话对象
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-3
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public <T> T getSession() {//获取会话对象
		return (T)session;
	}

	public void setSession(Object session) {
		this.session = session;
	}

	private static int callMax = 10000;// 最大呼叫10万次
	private int callCount = 0;
	
	static{
		if(zoreFetchCount == null || zoreFetchCount <= 0){
			zoreFetchCount = 1000;
		}
	}
	
	/**
	 * <li>说明：Creates new WebSocket instance using given socket for communication and
     * limiting handshake time to given milliseconds.
     * @param socket :socket that should be used for communication
     * @param timeout ：maximum time in milliseconds for the handshake
     * @param args :arguments that will be passed to handshake call
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-9-28
	 * <li>修改人： 
	 * <li>修改日期：
	 */	
	public Client(AsynchronousSocketChannel socketChannel, MasterServer sockectServer,ReadCompletionHandler readHandler,WriteCompletionHandler writeHandler)throws IOException{
	    this.sockectServer = sockectServer; 
		this.socketChannel = socketChannel;
		this.readHandler = readHandler;
		this.writeHandler = writeHandler;
		this.responseMsgs = new ConcurrentLinkedQueue<Response>();// 发送给客户端的消息
		this.responseMsgsNotCode = new ConcurrentLinkedQueue<Response>();// 发送给客户端的消息
		this.requestMsgs =  new ConcurrentLinkedQueue<String>();// 客户端发送的消息
		this.bizObjects =  new ConcurrentLinkedQueue<HashMap<String, String>>();// 客户请求业务处理对象
		this.requestWithFile = new Request();
	}

	/**
	 * 
	 * <li>方法名：addRequestMsg
	 * <li>@param msg
	 * <li>返回类型：void
	 * <li>说明：添加信息到请求信息中
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-9-29
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void addRequestMsg(String msg){
		this.requestMsgs.add(msg);
	}

	/**
	 * 
	 * <li>方法名：addResponseMsg
	 * <li>@param msg
	 * <li>返回类型：void
	 * <li>说明：将响应信息添加到响应信息集合中
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-9-29
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void addResponseMsg(Response msg){
		this.responseMsgsNotCode.add(msg);
	}	

	/**
	 * 
	 * <li>方法名：close
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：关闭链接
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-12-11
	 * <li>修改人： 
	 * <li>修改日期：
	 * @throws IOException 
	 */
	public void close(){		
		if(this.requestWithFile != null && this.requestWithFile.isReadFile()){
			FileTransfer fr = requestWithFile.getFileReceiver();
			if(fr != null  && !fr.finishWrite()){
				fr.forceClose();// 强制关闭删除损害的文件
			}
		}
				
		try{
			this.sockectServer.clearSocket(index);// 清除
			this.socketChannel.close();		
		} catch (ClosedChannelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{			
			try{
				socketChannel.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 
	 * <li>方法名：readMonitor
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void readMonitor(){
		try{
			if (this.socketChannel.isOpen()){
				this.byteBuffer = BufferPool.getInstance().getBuffer();
	            this.socketChannel.read(this.byteBuffer, this, this.readHandler);  
	        } else {  
	            log.info("Session Or Channel has been closed");  
	        }
		}catch(Exception e){
			this.close();
		}
	}
	
	/**
	 * 
	 * <li>方法名：processBlankRead
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：处理读取不到数据的情况
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void processBlankRead(){
		if(this.readCount >= zoreFetchCount){// 空读超过指定次数，关闭链接，返回
			log.info("the max count read: " + this.readCount);
			this.close();
		}
	}
	
	/**
	 * 
	 * <li>方法名：readRequest
	 * <li>@param byteBuffer
	 * <li>@return
	 * <li>返回类型：boolean
	 * <li>说明：读取请求数据
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-2-2
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public boolean readRequest(){
		boolean returnValue = false;// 返回值	
								
		byteBuffer.flip();
		this.decoderHandler.process(byteBuffer,this);// 解码处理				
		byteBuffer.clear();	
		BufferPool.getInstance().releaseBuffer(this.byteBuffer);
		this.readDataFlag = true;// 将读取数据标识设置为真
		
		if(requestWithFile.isReadFile()){// 是否读取文件
			if(requestWithFile.readFinish()){// 是否读取完毕文件
			//if(requestWithFile.getFileReceiver().finishWrite()){// 是否读取完毕文件
				returnValue = true;
				if(MasterServer.keepConnect){//长连接
					this.readMonitor();
				}
			}else{// 没有读取完毕文件，注册通道，继续读取
				if(MasterServer.keepConnect){//长连接
					this.readMonitor();
				}else{
					try{
						this.readMonitor();
					}catch(Exception e){
						e.printStackTrace();
						this.close();
						returnValue = false;
						this.requestWithFile.getFileReceiver().close();// 关闭文件
						return returnValue;
					}
					
					this.inRead.compareAndSet(true, false);
				}
				
				returnValue = false;// 将文件内容读取完后再进行处理
			}
		}else{// 普通请求，没有上传文件
			returnValue = true;
			if(MasterServer.keepConnect){//长连接
				this.readMonitor();
			}
		}		
		
		/*if(returnValue){// 读取完毕放入处理队列
			HashMap<String, String> requestData = requestWithFile.getRequestData();
			if(requestData != null){
				this.getBizObjects().add(requestData);
			}
		}*/
		
		return returnValue;
	}
	
	/**
	 * 
	 * <li>方法名：addRequest
	 * <li>@param requestData
	 * <li>返回类型：void
	 * <li>说明：将请求参数放入到队列中
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-11-27
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void addRequest(HashMap<String, String> requestData){
		this.getBizObjects().add(requestData);
	}
	
	/**
	 * 
	 * <li>方法名：process
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：对链接的一次请求进行处理
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-2-9
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void process(){
		try{
			this.processHandler.process(this);// 业务处理
			if(!this.responseMsgsNotCode.isEmpty()){// 不为空进行写出信息
				this.coderHandler.process(this);// 协议编码处理
				this.writeMessage();// 回写数据
			}
		}catch(Exception e){
			e.printStackTrace();
			this.close();
		}
	}
	
	/**
	 * 
	 * <li>方法名：writeMessage
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：写入数据
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void writeMessage(){		
		if(!this.responseMsgs.isEmpty()){
			Response response = this.responseMsgs.poll();
			if(response != null){
				if(this.inWrite.compareAndSet(false, true)){
					response.write(this);
				}
			}
		}		
	}

	public boolean isHandShak() {
		return handShak;
	}

	public void setHandShak(boolean handShak) {
		this.handShak = handShak;
	}

	public MasterServer getSockectServer() {
		return sockectServer;
	}

	public void setSockectServer(MasterServer sockectServer) {
		this.sockectServer = sockectServer;
	}
	
	/**
	 * 
	 * <li>方法名：getSockector
	 * <li>@param key
	 * <li>@return
	 * <li>返回类型：Sockector
	 * <li>说明：根据selection key 获取业务处理相关对象
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-11-9
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public static Client getSockector(SelectionKey key){
		Object attachment = key.attachment();
		if(attachment instanceof Client){
			return (Client)attachment;
		}else{
			return null;
		}
	}

	/**
	 * 
	 * <li>方法名：registeHandler
	 * <li>@param coderHandler
	 * <li>@param decoderHandler
	 * <li>@param processHandler
	 * <li>返回类型：void
	 * <li>说明：给通信通道注册编码器，解码器，业务处理器
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-11-18
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void registeHandler(CoderHandler coderHandler, DecoderHandler decoderHandler, ProcessHandler processHandler){
		this.coderHandler = coderHandler;
		this.decoderHandler = decoderHandler;
		this.processHandler = processHandler;
	}

	public ConcurrentLinkedQueue<HashMap<String, String>> getBizObjects() {
		return bizObjects;
	}

	public void setBizObjects(ConcurrentLinkedQueue<HashMap<String, String>> bizObjects) {
		this.bizObjects = bizObjects;
	}

	public ConcurrentLinkedQueue<String> getRequestMsgs() {
		return requestMsgs;
	}

	public void setRequestMsgs(ConcurrentLinkedQueue<String> requestMsgs) {
		this.requestMsgs = requestMsgs;
	}

	public ConcurrentLinkedQueue<Response> getResponseMsgs() {
		return responseMsgs;
	}

	public void setResponseMsgs(ConcurrentLinkedQueue<Response> responseMsgs) {
		this.responseMsgs = responseMsgs;
	}
	
	/**
	 * 
	 * <li>方法名：getIp
	 * <li>@return
	 * <li>返回类型：String
	 * <li>说明：获取客户端的ip地址
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-11-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public String getIp(){		
		try {
			return this.socketChannel.getRemoteAddress().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 
	 * <li>方法名：getAddress
	 * <li>@return
	 * <li>返回类型：String
	 * <li>说明：获取连接另外一端的地址
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-28
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public String getRouteAddress(){		
		try {
			return this.socketChannel.getRemoteAddress().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 
	 * <li>方法名：getLocalAddress
	 * <li>@return
	 * <li>返回类型：String
	 * <li>说明：获取连接的本端地址
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-28
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public String getLocalAddress(){				
		try {
			return this.socketChannel.getLocalAddress().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public AtomicBoolean getInRead() {
		return inRead;
	}

	public Request getRequestWithFile() {
		return requestWithFile;
	}

	public void setRequestWithFile(Request requestWithFile) {
		this.requestWithFile = requestWithFile;
	}

	public String getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(String protocolVersion) {
		this.protocolVersion = protocolVersion;
	}
	
	/**
	 * 
	 * <li>方法名：getProtocolVersionInt
	 * <li>@return
	 * <li>返回类型：int
	 * <li>说明：返回整数标识的版本号码
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-9-12
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public int getProtocolVersionInt(){
		return Integer.valueOf(this.protocolVersion);
	}
	
	/**
	 * 
	 * <li>方法名：broadCastMessage
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：往连接的对端发送消息
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-2
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void sendMessage(Response msg){
		try{
			this.responseMsgsNotCode.add(msg);
			this.coderHandler.process(this);// 编码消息
			//log.info(msg.getBody());
			this.writeMessage();
		}catch(Exception e){
			e.printStackTrace();
			this.close();
		}
	}
	
	public CoderHandler getCoderHandler() {
		return coderHandler;
	}

	public void setCoderHandler(CoderHandler coderHandler) {
		this.coderHandler = coderHandler;
	}

	public DecoderHandler getDecoderHandler() {
		return decoderHandler;
	}

	public void setDecoderHandler(DecoderHandler decoderHandler) {
		this.decoderHandler = decoderHandler;
	}

	public ProcessHandler getProcessHandler() {
		return processHandler;
	}

	public void setProcessHandler(ProcessHandler processHandler) {
		this.processHandler = processHandler;
	}

	public void send(Response msg){
		try{
			this.responseMsgs.add(msg);
			this.writeMessage();
		}catch(Exception e){
			e.printStackTrace();
			this.close();
		}
	}
	
	public void sendDirectMessage(Response msg){
		try{
			this.responseMsgsNotCode.add(msg);
			this.coderHandler.process(this);// 编码消息
			//this.sendMsgs();
			this.writeMessage();
		}catch(Exception e){
			e.printStackTrace();
			this.close();
		}
	}
	
	public boolean isReadDataFlag() {
		return readDataFlag;
	}

	public void setReadDataFlag(boolean readDataFlag) {
		this.readDataFlag = readDataFlag;
	}
	
	/**
	 * 
	 * <li>方法名：handShak
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：执行握手请求处理
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void requestHandShak(){
		this.isClient = true;// 连接的客户端
		this.coderHandler.handShak(this);
	}
	
	public Integer getReadCount() {
		return readCount;
	}

	public void setReadCount(Integer readCount) {
		this.readCount = readCount;
	}
}
