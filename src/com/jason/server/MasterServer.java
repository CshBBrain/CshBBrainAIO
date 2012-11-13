package com.jason.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.SocketOptions;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jason.Config;
import com.jason.server.clusters.ClustersCoder;
import com.jason.server.clusters.ClustersDecoder;
import com.jason.server.clusters.ClustersProcesser;
import com.jason.server.hander.CoderHandler;
import com.jason.server.hander.DecoderHandler;
import com.jason.server.hander.ProcessHandler;
import com.jason.util.MyStringUtil;

/**
 * 
 * <li>类型名称：
 * <li>说明：基于JAVA NIO 的面向TCP/IP的,非阻塞式Sockect服务器框架主类
 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
 * <li>创建日期：2011-11-9
 * <li>修改人： 
 * <li>修改日期：
 */
public class MasterServer {
	private static Log log = LogFactory.getLog(MasterServer.class);// 日志记录器
	// 系统配置参数参数名
	private static final String PORT_NAME = "port";// 端口
	private static final String MAX_PRIORITY = "maxPriority";// 线程优先级
	private static final String REQUEST_WORKER = "requestWorker";// 请求处理线程因子/核
	private static final String MONITOR_WORKER = "monitorWorker";// 数据接收监听线程因子/核
	private static final String SOCKECT_SEND_BUFFER_SIZE = "sockectSendBufferSize";// sockect读取缓冲区大小
	private static final String SOCKECT_RECVEID_BUFFER_SIZE = "sockectReceiveBufferSize";// sockect发送缓冲区大小
	
	// 集群相关参数
	private static final String CLUSTERS_SWITCH = "clustersSwitch";// 集群开关
	private static final String CLUSTERS_PORT = "clustersPort";// 集群端口
	private static final String CLUSTERS_ROLE = "clustersRole";// 集群服务器责任
	private static final String MASTER_SERVER = "masterServer";// 集群管理服务器
	
	private static final String BROAD_SWITCH = "broadSwitch";// 广播开关
	private static final String KEEP_CONNECT = "keepConnect";
	private static final String TIME_OUT = "timeOut";// timeout 参数
	public static boolean keepConnect = false;// 是否保持链接
	
	private int sockectSendBufferSize = 64;// 默认为64k
	private int sockectReceiveBufferSize = 5;// 默认为5k
	private Boolean broadSwitch = false;//默认广播开关关闭
	private Integer timeOut = 10;// 默认没有收到数据的超时阀值为10分钟，超过指定时间没有收到数据立即关闭连接
	
	// 定义编码处理器，业务处理器，解码处理器
	private CoderHandler coderHandler;// 编码处理器
	private DecoderHandler decoderHandler;// 解码处理器
	private ProcessHandler processHandler;// 业务处理器
	
	// 定义集群编码处理器，集群处理器和集群解码处理器
	private ClustersCoder clustersCoder;// 集群编码处理器
	private ClustersDecoder clustersDecoder;// 集群解码处理器
	private ClustersProcesser clustersProcesser;// 集群处理器
	private Client localClient;// 连接到集群服务器的客户端
	
	private static volatile LinkedBlockingQueue<Response> broadMessages = new LinkedBlockingQueue<Response>();// 广播消息队列 
	private String stockData;// 股指数据，多个股指之间用逗号分隔
	
	private ConcurrentHashMap<Object, Client> clients = new ConcurrentHashMap<Object, Client>();// 客户端链接映射表
	private ConcurrentHashMap<Object, Client> clustersClients = new ConcurrentHashMap<Object, Client>();// 集群服务上的所有节点服务器客户端链接映射表
	private ConcurrentHashMap<Object, Client> localClients = new ConcurrentHashMap<Object, Client>();// 连接到其他非管理服务器上的所有本地链接映射表
	
	private final AtomicInteger connectIndex = new AtomicInteger();// 连接序号
	private final AtomicInteger keyIndex = new AtomicInteger();// 连接序号
	
	private volatile Integer port;// 业务处理服务器端口
	
	// 服务器集群参数变量
	private Boolean clustersSwitch = false;// 集群开关,默认不开启
	private Integer clustersPort = 9292;// 集群端口，默认为9292
	private Integer clustersRole = 1;// 集群服务器责任，默认为普通业务服务器
	private String masterServer;// 集群管理服务器
	
	private Thread connectMonitor;// 连接处理线程
	private Thread clustersMonitor;// 集群连接处理线程
	
	private int coreCount = 1;// CPU内核数量
	private int readerWriterCount = 2;// 读写监听线程数量
	private int workerCount = 1;// 工作线程数量
	
	private Thread broadMessageThread;// 发送广播消息的线程
	private Thread clustersClientThread;// 发送广播消息的线程
	private AsynchronousServerSocketChannel server;// 异步服务端
	private AsynchronousServerSocketChannel clustersServer;// 异步服务端
	private AcceptCompletionHandler acceptHandler;// 连接处理器
	private ReadCompletionHandler readHandler;// 读取处理器
	private WriteCompletionHandler writeHandler;// 回写处理器
	private volatile LinkedBlockingQueue<SelectionKey> taskQueue = new LinkedBlockingQueue<SelectionKey>();// 读取队列

	private Thread clientMonitor;// 客户端连接数据接收状况监听，对于超过时限没有接收到数据的客户端关闭连接
	
	private volatile BlockingQueue<Worker> workers;// 读取的工作线程	
	private volatile ArrayList<Worker> workersList;// 读取线程列表
		
	private volatile boolean noStopRequested = true;// 循环控制变量
	
	public MasterServer(CoderHandler coderHandler, DecoderHandler decoderHandler, ProcessHandler processHandler)throws IOException{
		// 设置端口
		this.port = Config.getInt(PORT_NAME);// 从配置中获取端口号
		if(this.port == null){
			this.port = 9090;// 设置默认端口为9090
		}
		
		// 设置超时
		this.timeOut = Config.getInt(TIME_OUT);// 获取配置中的超时设置
		if(this.timeOut == null){
			this.timeOut = 10;// 设置默认超时时限为10分钟
		}
		
		// 设置线程优先级
		Integer serverPriority = Config.getInt(MAX_PRIORITY);// 获取配置中的线程优先级
		if(serverPriority == null){
			serverPriority = 5;// 设置默认线程优先级
		}
		
		// 设置请求读取处理线程每个核心的线程数
		Integer requestWorker = Config.getInt(REQUEST_WORKER);
		if(requestWorker == null){
			requestWorker = 5;
		}
		
		// 设置请求响应处理线程每个核心的线程数
		Integer monitorWorker = Config.getInt(MONITOR_WORKER);
		if(monitorWorker == null){
			monitorWorker = 1;
		}
		
		// 设置sockect数据接收缓冲区大小
		Integer receiveBuffer = Config.getInt(SOCKECT_RECVEID_BUFFER_SIZE);
		if(receiveBuffer != null){
			this.sockectReceiveBufferSize = receiveBuffer;
		}
		
		// 设置sockect数据读取缓冲区大小
		Integer sendBuffer = Config.getInt(SOCKECT_SEND_BUFFER_SIZE);
		if(sendBuffer != null){
			this.sockectSendBufferSize = sendBuffer;
		}
		
		//设置是否保存长连接
		Integer keepAlive = Config.getInt(KEEP_CONNECT);
		if(keepAlive != null){
			keepConnect = (keepAlive == 1 ? true : false);
		}
		
		// 设置广播开关
		Boolean broad = Config.getBoolean(BROAD_SWITCH);
		if(broad != null){
			this.broadSwitch = broad;
		}
		
		// 设置集群开关
		Boolean clustersSwitch = Config.getBoolean(CLUSTERS_SWITCH);
		if(clustersSwitch != null){
			this.clustersSwitch = clustersSwitch;
		}
		
		// 设置集群端口
		Integer clustersPort = Config.getInt(CLUSTERS_PORT);
		if(clustersPort != null){
			this.clustersPort = clustersPort;
		}
		
		// 设置服务器的职责
		Integer clustersRole = Config.getInt(CLUSTERS_ROLE);
		if(clustersRole != null){
			this.clustersRole = clustersRole;
		}
		
		// 设置集群中的上级服务器
		String masterServer = Config.getStr(MASTER_SERVER);
		if(masterServer != null){
			this.masterServer = masterServer;
		}
		
		// 设置解码器，编码器和业务处理器
		this.coderHandler = coderHandler;
		this.decoderHandler = decoderHandler;
		this.processHandler = processHandler;
		
		this.coreCount = Runtime.getRuntime().availableProcessors();
		this.readerWriterCount = coreCount * monitorWorker;
		this.workerCount = coreCount * requestWorker;
		
		this.workers = new ArrayBlockingQueue<Worker>(this.workerCount);
		this.workersList = new ArrayList<Worker>(this.workerCount);//响应处理线程列表
		
		for(int i = 0; i < this.workerCount; ++i){
			workersList.add(new Worker(serverPriority,workers));			
		}
		
		if(this.broadSwitch){// 根据开关是否创建广播线程
			this.createBroadMessageThread(serverPriority);//创建广播消息线程
		}
		
		this.startMonitor();// 创建业务连接建立监听线程
		
		if(this.timeOut > 0){
			this.createClientMonitorThread(serverPriority);// 创建客户端数据接收状态监听线程
		}
		
		// 处理集群初始化
		if(this.clustersSwitch){
			// 创建集群编码解码器和集群业务处理器
			this.clustersCoder = new ClustersCoder();
			this.clustersDecoder = new ClustersDecoder();
			this.clustersProcesser = new ClustersProcesser();
			
			this.startClustersMonitor();
			
			// 连接到自己的直接管理服务器节点
			if((this.clustersRole == 1 || this.clustersRole == 3) && !MyStringUtil.isBlank(this.masterServer)){
				this.createClustersClientThread(serverPriority);// 创建集群通信客户端消息处理线程
			}
		}
	}
	
	/**
	 * 
	 * <li>方法名：createClustersClientThread
	 * <li>@param serverPriority
	 * <li>返回类型：void
	 * <li>说明：创建集群服务器客户端处理线程
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	private void createClustersClientThread(int serverPriority) {
		// 集群服务器通信处理客户端线程
		Runnable writeDistributeRunner = new Runnable(){
			public void run(){
				try{
					startClustersMessage();
				}catch(Exception e){
					e.printStackTrace();
				}
			}			
		};
		
		this.clustersClientThread = new Thread(writeDistributeRunner);
		this.clustersClientThread.setName("集群通信客户端消息处理线程");
		this.clustersClientThread.setPriority(serverPriority);
		this.clustersClientThread.start();
		log.info("集群通信客户端消息处理线程创建完毕");
	}
	
	/**
	 * 
	 * <li>方法名：startClustersMessage
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：开始处理集群通信客户端的消息
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	protected void startClustersMessage(){
		Boolean isConnect = true;// 是否连接集群服务器
		AsynchronousSocketChannel socketChannel = null;// 连接客户端通道
		
		String[] address = this.masterServer.split(":");// 集群服务器地址
		if(address.length < 2){// 集群服务器地址错误
			log.info("集群服务器地址配置错误，集群服务器地址格式为：ip:port，例如：192.168.2.32：9090，请重新配置后再启动");
			return;
		}
		 
		if(MyStringUtil.isBlank(address[0]) || MyStringUtil.isBlank(address[1])){// 集群服务器ip地址错误
			log.info("集群服务器ip地址配置错误，集群服务器ip地址格式为：ip:port，例如：192.168.2.32：9090，请重新配置后再启动");
			return;
		}					
		 
		InetSocketAddress socketAddress = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
		 
		while(noStopRequested){
			try{				
				if(isConnect && socketChannel == null){// 第一次连接或连接断开都需要连接到集群服务器上 
				try{					 
					 if(socketChannel != null && !socketChannel.isOpen()){
						 socketChannel.close();  
						 log.info("已经连接到集群服务器，但连接断开，重新连接");  
					 }
					 
					 socketChannel = AsynchronousSocketChannel.open();

					 /*socketChannel.socket().setSoTimeout(0);
					 socketChannel.socket().setTcpNoDelay(true);
			         socketChannel.socket().setReuseAddress(true);*/ 
			         
			        /* socketChannel.socket().setReceiveBufferSize(this.sockectReceiveBufferSize * 1024);// 设置接收缓存
			         socketChannel.socket().setSendBufferSize(this.sockectSendBufferSize * 1024);// 设置发送缓存
*/			 		
			         socketChannel.connect(socketAddress);
			         
			         Client client = new Client(socketChannel,this,readHandler,writeHandler);			         
			         client.registeHandler(clustersCoder, clustersDecoder, clustersProcesser);// 注册业务处理器
			         this.localClient = client;			        
			         client.requestHandShak();// 发起握手
			         
			        }catch(ClosedChannelException e){  
			            log.info("连接集群服务器失败");  
			        }catch(IOException e){  
			            log.info("连接集群服务器失败");  
			        }
				}else{// 处理消息监听					
					if(!isConnect && !socketChannel.isOpen()){// 到集群服务器的连接端口，重新连接
						isConnect = true;
						socketChannel.connect(socketAddress);						
						continue;
					}
					
					if(!isConnect && socketChannel.isOpen()){
						// 汇报本服务的负载情况到集群管理服务器
						if(this.localClient != null){// 已经连接好，等待通信
							StringBuilder sb = new StringBuilder();
							sb.append("节点服务器：").append(this.localClient.getLocalAddress()).append("\r\n")
							.append("服务器CPU内核数量：").append(this.coreCount).append("\r\n")
							.append("服务器读写监听线程数量：").append(this.readerWriterCount).append("\r\n")
							.append("服务器工作线程数量：").append(this.workerCount).append("\r\n")
							.append("活跃连接客户端数量：").append(this.clients.keySet().size()).append("\r\n")
							.append("活跃集群连接客户端数量：").append(this.clustersClients.keySet().size()).append("\r\n")
							.append("活跃本地连接客户端数量：").append(this.localClients.keySet().size()).append("\r\n");
							
							log.info(sb.toString());
							StringBuilder msg = new StringBuilder("action=1&");
							msg.append("coreCount=").append(this.coreCount).append("&")
							.append("readerWriterCount=").append(this.readerWriterCount).append("&")
							.append("workerCount=").append(this.workerCount).append("&")
							.append("clientCount=").append(this.clients.keySet().size()).append("&")
							.append("clustersCount=").append(this.clustersClients.keySet().size()).append("&")
							.append("port=").append(this.port).append("&")
							.append("localCount=").append(this.localClients.keySet().size());
							
							Response response = new Response();
							response.setBody(msg.toString());
							this.localClient.sendMessage(response);
							//coreCount=4&readerWriterCount=8&workerCount=32&clientCount=10000&clustersCount=5&localCount=4&port=9191
						}
						Thread.sleep(30 * 1000);//10分钟检查一次
					}
				}				
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}		
	}
		
	/**
	 * 
	 * <li>方法名：createBroadMessageThread
	 * <li>@param serverPriority
	 * <li>返回类型：void
	 * <li>说明：创建广播线程
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	private void createBroadMessageThread(int serverPriority) {
		// 消息广播线程
		Runnable writeDistributeRunner = new Runnable(){
			public void run(){
				try{
					startBroadMessage();
				}catch(Exception e){
					e.printStackTrace();
				}
			}			
		};
		
		this.broadMessageThread = new Thread(writeDistributeRunner);
		this.broadMessageThread.setName("消息广播线程");
		this.broadMessageThread.setPriority(serverPriority);
		this.broadMessageThread.start();
		log.info("消息广播线程创建完毕");
	}
	
	/**
	 * 
	 * <li>方法名：addWorkers
	 * <li>@param count
	 * <li>返回类型：void
	 * <li>说明：
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public Boolean addWorkers(Integer count){
		for(int i = 0; i < this.workerCount; ++i){
			workersList.add(new Worker(5,workers));			
		}
		
		return true;
	}

	/**
	 * 
	 * <li>方法名：startBroadMessage
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：开始处理广播消息
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	protected void startBroadMessage(){		
		while(noStopRequested){
			try{
				Response msg = broadMessages.take();//获取广播消息
				Iterator<Client> it = this.clients.values().iterator();
				boolean isCode = false;
				
				while(it.hasNext()){
					//log.info(msg.getBody());
					Client socketer = it.next();
					if(!isCode){
						isCode = true;
						msg.codeMsg(socketer);
					}
					
					socketer.send(Response.msgRespose(msg));
				}
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}		
	}
	
	/**
	 * 
	 * <li>方法名：addBroadMessage
	 * <li>@param msg
	 * <li>返回类型：void
	 * <li>说明：将要广播的消息放到广播线程中，有广播线程负责广播
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-10-9
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public static void addBroadMessage(Response msg){
		broadMessages.add(msg);
	}
	
	// 创建链接调度线程
	private void createClientMonitorThread(int serverPriority){
		//创建监听线程
		noStopRequested = true;
		Runnable monitorRunner = new Runnable(){
			public void run(){
				try{
					startClientMonitor();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		};
		
		this.clientMonitor = new Thread(monitorRunner);
		this.clientMonitor.setName("客户端数据接收状况监听主线程");
		log.info("客户端数据接收状况监听主线程创建成功");
		this.clientMonitor.setPriority(serverPriority);
		this.clientMonitor.start();
	}
	
	/**
	 * 
	 * <li>方法名：startClientMonitor
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：开始执行客户端数据发送状况监听
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-12
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	private void startClientMonitor(){			
		while(noStopRequested){
			try {
				if(this.timeOut > 0){// 超时阀值				
					Iterator<Object> it = clients.keySet().iterator();
					while(it.hasNext()){
						Object key = it.next();
						Client client = clients.get(key);
						if(!client.isReadDataFlag()){// 超时没有收到数据
							client.close();// 关闭连接
							clients.remove(key);// 从映射表中删除连接
						}else{
							client.setReadDataFlag(false);// 将读取数据标识设置为false
						}
					}
					
					this.clientMonitor.sleep(this.timeOut * 60 * 1000);// 暂停10分钟
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 
	 * <li>方法名：startClustersMonitor
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：开始监听集群服务器节点中的连接请求
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-21
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	private void startClustersMonitor(){
		try{
			// 创建共享资源池
			AsynchronousChannelGroup resourceGroup = AsynchronousChannelGroup.withCachedThreadPool(Executors.newCachedThreadPool(),this.workerCount);  
			
			// 创建服务端异步socket
			this.clustersServer = AsynchronousServerSocketChannel.open(resourceGroup);  			
			this.clustersServer.bind(new InetSocketAddress(this.clustersPort), 10); 			
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Server start up fail");
		}
	}
	
	/**
	 * 
	 * <li>方法名：startMonitor
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：开始监听业务请求处理
	 * <li>创建人：CshBBrain;技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2011-9-28
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	private void startMonitor(){		
		try{
			// 创建共享资源池
			AsynchronousChannelGroup resourceGroup = AsynchronousChannelGroup.withCachedThreadPool(Executors.newCachedThreadPool(),this.workerCount);  
			
			// 创建服务端异步socket
			this.server = AsynchronousServerSocketChannel.open(resourceGroup); 			
			this.server.bind(new InetSocketAddress(this.port), 100);  
			this.acceptHandler = new AcceptCompletionHandler(1);
			this.readHandler = new ReadCompletionHandler(this);
			this.writeHandler = new WriteCompletionHandler();
			
			
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Server start up fail");
		}
	}
	
	/**
	 * 
	 * <li>方法名：startServer
	 * <li>
	 * <li>返回类型：void
	 * <li>说明：启动服务器监听
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-30
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void startServer(){
		try{
			this.server.accept(this, this.acceptHandler);// 创建监听处理器			
			log.info("服务器准备就绪，等待请求到来");
			
			if(this.clustersSwitch){
				this.clustersServer.accept(this, new AcceptCompletionHandler(2));// 创建监听处理器			
				log.info("集群服务器准备就绪，等待请求到来");
			}
		}catch(Exception e){
			log.info(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, MasterServer>{		
	    private Integer serverType = 1;// 1: 节点服务器，2：集群管理服务器
		public AcceptCompletionHandler(Integer serverType){
			this.serverType = serverType;
		}
		
		public void cancelled(MasterServer attachment) {  
			log.warn("Accept operation was canceled");  
	    }  

	    public void completed(AsynchronousSocketChannel socketChannel,MasterServer attachment) {  
	        try{  
	        	log.info("Accept connection from "+ socketChannel.getRemoteAddress());
	        	//socketChannel.setOption(SocketOptions.SO_SNDBUF, sockectReceiveBufferSize * 1024);
	        	socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
	        	socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, sockectSendBufferSize * 1024);
	        	socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, sockectReceiveBufferSize * 1024);
	            Client client = new Client(socketChannel,attachment,readHandler,writeHandler);
	            Integer index = 0;
	            
	            switch(this.serverType){	            	
		            case 1:
		            	if(MasterServer.this.clustersRole == 2){
		            		client.registeHandler(coderHandler, decoderHandler, clustersProcesser);// 注册业务处理器
		            	}else{
		            		client.registeHandler(coderHandler, decoderHandler, processHandler);// 注册业务处理器
		            	}
		            	index = keyIndex.incrementAndGet();
		 	            client.setIndex(index);// 设置索引
		 				clients.put(index, client);// 放入到连接中
		            	break;
		            case 2:
		            	client.registeHandler(clustersCoder, clustersDecoder, clustersProcesser);// 注册业务处理器
		            	MasterServer.this.clustersClients.put(client.getRouteAddress(), client);
		            	break;
		            default:
		            	client.registeHandler(coderHandler, decoderHandler, processHandler);// 注册业务处理器
		            	index = keyIndex.incrementAndGet();
		 	            client.setIndex(index);// 设置索引
		 				clients.put(index, client);// 放入到连接中
	            }	           
				
	            client.readMonitor();// 开始监听读取
	        } catch (Exception e) {  
	            e.printStackTrace();  
	            log.error(e.getMessage());  
	        }finally {  
	        	attachment.getServer().accept(attachment,this);// 监听新的请求
	        }  
	    }
	    
	    public void failed(Throwable exc, MasterServer attachment) {    	  
	        try{  
	        	log.error(exc.getMessage());
	        }finally{  
	        	attachment.getServer().accept(attachment,this);// 监听新的请求
	        }  
	    } 
	}	
	
	public boolean isAlive(){
		return this.connectMonitor.isAlive();
	}
    
    public void closeServer(){
		//this.serverSocket.close();//关闭链接
		
		//停止接收主线程
		this.noStopRequested = false;
		this.connectMonitor.interrupt();		
    }

	public String getStockData() {
		return stockData;
	}

	public void setStockData(String stockData) {
		this.stockData = stockData;
	}
	
	/**
	 * 
	 * <li>方法名：clearSocket
	 * <li>@param index
	 * <li>返回类型：void
	 * <li>说明：删除指定的连接
	 * <li>创建人：CshBBrain, 技术博客：http://cshbbrain.iteye.com/
	 * <li>创建日期：2012-10-22
	 * <li>修改人： 
	 * <li>修改日期：
	 */
	public void clearSocket(Object index){
		Client client = this.localClients.remove(index);// 从本地连接中清除
		if(client != null){
			return;
		}
		
		client = this.clustersClients.remove(index);// 从集群连接中清除
		if(client != null){
			return;
		}
				
		client = this.clients.remove(index);// 从客户端连接中清除
		if(client != null){
			return;
		}
	}
	
	public Client getLocalClient() {
		return localClient;
	}

	public void setLocalClient(Client localClient) {
		this.localClient = localClient;
	}
	
	public ConcurrentHashMap<Object, Client> getClustersClients() {
		return clustersClients;
	}

	public void setClustersClients(ConcurrentHashMap<Object, Client> clustersClients) {
		this.clustersClients = clustersClients;
	}
	
	public ConcurrentHashMap<Object, Client> getLocalClients() {
		return localClients;
	}

	public void setLocalClients(ConcurrentHashMap<Object, Client> localClients) {
		this.localClients = localClients;
	}
	public AsynchronousServerSocketChannel getServer() {
		return server;
	}

	public void setServer(AsynchronousServerSocketChannel server) {
		this.server = server;
	}
	
	public BlockingQueue<Worker> getWorkers() {
		return workers;
	}

	public void setWorkers(BlockingQueue<Worker> workers) {
		this.workers = workers;
	}
}
