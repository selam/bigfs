package org.bigfs.fs.service;

import java.io.IOError;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.SimpleCondition;
import org.bigfs.concurrent.ThreadPoolExecutorFactory;
import org.bigfs.fs.concurrent.BigFSConnection;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.messages.BigFSCreateDirectoryMessage;
import org.bigfs.fs.messages.BigFSCreateDirectoryMessageHandler;
import org.bigfs.fs.messages.BigFSCreateFileMessage;
import org.bigfs.fs.messages.BigFSCreateFileMessageHandler;
import org.bigfs.fs.messages.BigFSDiskStatusMessage;
import org.bigfs.fs.messages.BigFSDiskStatusMessageHandler;
import org.bigfs.fs.messages.BigFSDiskStatusMessageReply;
import org.bigfs.fs.messages.BigFSDiskStatusMessageReplyHandler;
import org.bigfs.internode.configuration.MessagingConfiguration;
import org.bigfs.internode.service.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class BigFSServer
{
    private static final Logger logger = LoggerFactory.getLogger(BigFSServer.class);
        
    public static int PROTOCOL_MAGIC = 0xBFDDACFF;
    
    private final InetAddress listenAddress;
    private final int listenPort; 
    
    private final SimpleCondition listenGate;
     
    private static final ExecutorService clientExecutor = new ThreadPoolExecutor(
            16,
            Integer.MAX_VALUE,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>()
    );
    
    private final List<SocketThread> socketThreads = Lists.newArrayList();
    
    public BigFSServer(InetAddress listenAddress, int listenPort) throws Exception //throws Exception
    {
        this.listenAddress = listenAddress;
        this.listenPort = listenPort;
        listenGate = new SimpleCondition();
        
        // setting up messaging service for remote node disk information        
        system_add_keyspace();
        
        MessagingService.registerMessageSerializer(BigFSCreateFileMessage.messageType, BigFSCreateFileMessage.serializer);
        MessagingService.registerMessageHandlers(BigFSCreateFileMessage.messageType, new BigFSCreateFileMessageHandler());

        MessagingService.registerMessageSerializer(BigFSCreateDirectoryMessage.messageType, BigFSCreateDirectoryMessage.serializer);
        MessagingService.registerMessageHandlers(BigFSCreateDirectoryMessage.messageType, new BigFSCreateDirectoryMessageHandler());

        
        MessagingService.registerMessageSerializer(BigFSDiskStatusMessage.messageType, BigFSDiskStatusMessage.serializer);
        MessagingService.registerMessageHandlers(BigFSDiskStatusMessage.messageType, new BigFSDiskStatusMessageHandler());        
        MessagingService.registerMessageSerializer(BigFSDiskStatusMessageReply.messageType, BigFSDiskStatusMessageReply.serializer);
        MessagingService.registerMessageHandlers(BigFSDiskStatusMessageReply.messageType, new BigFSDiskStatusMessageReplyHandler());        
        
        MessagingService.registerMessageGroupExecutor(BigFSCreateFileMessage.messageGroup, ThreadPoolExecutorFactory.multiThreadedExecutor("BigFSFileOperationExecutor", "BigFSFileOperationExecutor", 80));
        MessagingService.registerMessageGroupExecutor(BigFSDiskStatusMessage.messageGroup, ThreadPoolExecutorFactory.multiThreadedExecutor("BigFSInternalMessagingExecutor", "BigFSInternalMessagingExecutor", 80));
        MessagingConfiguration.setListenAddress(listenAddress);
        MessagingConfiguration.setPort(BigFSConfiguration.getRPCPort());
        MessagingService.instance().listen();
        
    }

    

    
    public void stop()
    {
    	logger.info("we going to stop bigFS");

    	try
        {
    	    // close listening first
            for (SocketThread th : socketThreads)
                th.close();
            // wait stoping all clients
            clientExecutor.shutdown();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }        
    }
    

    public void start() throws RuntimeException
    {
    	 logger.info("We are start to listening "+listenAddress+" on "+listenPort); 
        for (ServerSocket ss : getServerSocket())
        {
            SocketThread th = new SocketThread(ss, "ACCEPT-" + ss.getInetAddress());
            th.start();
            socketThreads.add(th);
        }
        listenGate.signalAll();
    }
    
    
    private List<ServerSocket> getServerSocket() throws RuntimeException
    {
        final List<ServerSocket> ss = new ArrayList<ServerSocket>();


        ServerSocketChannel serverChannel = null;
        try
        {
            serverChannel = ServerSocketChannel.open();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        ServerSocket socket = serverChannel.socket();
        try
        {
            socket.setReuseAddress(true);
        }
        catch (SocketException e)
        {
            throw new RuntimeException("Insufficient permissions to setReuseAddress");
        }
        InetSocketAddress address = new InetSocketAddress(listenAddress, listenPort);
        try
        {
            socket.bind(address);
        }
        catch (BindException e)
        {
            if (e.getMessage().contains("in use"))
                throw new RuntimeException(address + " is in use by another process.  Change listen_address:storage_port in cassandra.yaml to values that do not conflict with other services");
            else if (e.getMessage().contains("Cannot assign requested address"))
                throw new RuntimeException("Unable to bind to address " + address
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            else
                throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
        logger.info("Starting BigFS Service on port {}",  listenPort);
        ss.add(socket);
        return ss;
    }
    
    
    
    private boolean system_add_keyspace() 
    {   
        logger.debug("add_keyspace");
        KsDef ks_def = new  KsDef();
        ks_def.setName(BigFSConfiguration.getKeyspace());
        ks_def.setStrategy_class("SimpleStrategy");
        Map<String, String> keyspace_options = new HashMap<String, String>();
        keyspace_options.put("replication_factor", "3");
        ks_def.setStrategy_options(keyspace_options);
        
        CfDef directories = new CfDef();
        directories.setKeyspace(ks_def.getName());
        directories.setName(BigFSConfiguration.getColumnFamily());
        directories.setKey_validation_class("UTF8Type");
        directories.setComparator_type("UTF8Type");        
        directories.setDefault_validation_class("UTF8Type");
        
        CfDef fileBlocks = new CfDef();
        fileBlocks.setKeyspace(ks_def.getName());
        fileBlocks.setName("FileBlocks");
        fileBlocks.setKey_validation_class("UTF8Type");
        fileBlocks.setComparator_type("UTF8Type");          
        fileBlocks.setDefault_validation_class("UTF8Type");
        
        ks_def.setCf_defs(Arrays.asList(directories, fileBlocks));
        
        try
        {
            ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);
            logger.info("key space not exists");
        }
        catch(InvalidRequestException e)
        {
            if(e.getMessage().contains("(\""+BigFSConfiguration.getKeyspace()+"\" conflicts with \""+BigFSConfiguration.getKeyspace()+"\")"))
            {
                logger.info("key space exists");
                
                return true;
            }
            
            logger.error("Fatal configuration error {}", e.getMessage());
            System.err.println(e.getMessage() + "\n unable to start server.  See log for stacktrace.");
            System.exit(1); 
            return false;
        }
        
        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cf : ks_def.cf_defs)
            {
                cf.unsetId(); 
                CFMetaData cfm = CFMetaData.fromThrift(cf);
                cfm.addDefaultIndexNames();
                cfDefs.add(cfm);
            }
            MigrationManager.announceNewKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()])));
            
            return true;
        }
        catch(ConfigurationException e)
        {
            logger.error("Fatal configuration error error", e);
            System.err.println(e.getMessage() + "\n unable to start server.  See log for stacktrace.");
            System.exit(1); 
            return false;
        }
        catch (RequestValidationException e)
        {
            logger.info("we catch an exception {}", e);
            return false;
        }   
    }

    public static void validateProcotolMagic(int magic) throws IOException 
    {   
        if (magic != PROTOCOL_MAGIC) 
        {
            throw new IOException("invalid protocol header");
        }
    }

    private static class SocketThread extends Thread
    {
        private final ServerSocket server;

        SocketThread(ServerSocket server, String name)
        {
            super(name);
            this.server = server;
        }

        public void run()
        {
            while (true)
            {
                try
                {
                    Socket socket = server.accept();
                    clientExecutor.execute(new BigFSConnection(socket));                    
                }
                catch (AsynchronousCloseException e)
                {
                    // this happens when another thread calls close().
                    logger.info("shutting down server thread.");
                    break;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        void close() throws IOException
        {
            server.close();
        }
    }
}
