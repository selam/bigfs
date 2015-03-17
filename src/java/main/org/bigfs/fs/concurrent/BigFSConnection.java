package org.bigfs.fs.concurrent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.bigfs.fs.directory.BigFSDirectoryManager;
import org.bigfs.fs.exceptions.BigFSException;
import org.bigfs.fs.file.BigFSFile;
import org.bigfs.fs.security.UserGroupInformation;
import org.bigfs.fs.service.BigFSServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * when client open the connection  it must send 4 byte protocol magic number,
 * after then it can send following commands, open, remove, rename, close, seek, read, write, ls
 *                                            1      2       3      4     5      6     7      8
 *                                            open modes, R 1, W 2
 *  error codes starts from -100 //
 *  all error codes must be explain on method comments
 * 
 * */

/*
Directories: {
    dirname: {        
        inodename: "type-user-group-perms-created_at"                
    }    
}

FileBlocks: {
    filename-blocks: {
       uuid: uuid
    }
    
}

Blocks: {
    blockId: {
       ip:true,
       ip:false
    }
}

FileAccessLogs: {
   filename: {
      TimeUUID: operation,user-group-created_at,      
   }
}

**/

public class BigFSConnection implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BigFSConnection.class);
    
    private final Socket socket;
    
    private DataInputStream in;
    private DataOutputStream out;
    
    private boolean _file_opened = false;
    private int _file_mode = -1; 
    private String _file_name = null;
    
    private BigFSFile file;
    
    private UserGroupInformation ugi;
    
    public BigFSConnection(Socket socket) throws IOException
    {
        logger.info("new connection from {}", socket.getInetAddress());
        this.socket = socket;
        this.socket.setTcpNoDelay(true);
        this.socket.setKeepAlive(true);        
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
     
    }
    
    
    public void run()
    {
        try 
        {
            // first 4 byte is the protocol magic number, maybe i delete this later            
            int header = in.readInt();            
            
            BigFSServer.validateProcotolMagic(header);
            
            this.ugi =  UserGroupInformation.read(in);
            
            logger.info("incoming connections from {} with {}", socket.getInetAddress().getHostAddress(), ugi);
         
            // now we wait to commands from client
            while(true){
                logger.info("waiting commands");
                int command = in.readInt();
                if(command == 0){
                    break;
                }
                logger.info("command is {}", command);
                switch(command) {
                    case 1:
                        createBigFSFile();
                    break;
                    case 2:
                        createBigFSDirectory();
                    break;
                    case 3:
                        listBigFSDirectory();
                    break;
                    case 4:
                        getBigFSInodeInformation();
                    break;
                    case 7:
                        writeBigFSFile();
                    break;
                }
            }
            
        }
        catch(IOException e)
        {
            logger.debug("IOException reading from socket; closing", e);
        }
        finally 
        {
            logger.info("closing socket {}", socket.getInetAddress());
            this.close();
        }
    }
    
    private void writeBigFSFile() throws IOException
    {

        
    }
    
    private void listBigFSDirectory()  throws IOException
    {
        String path = in.readUTF();
        
        try
        {
            BigFSDirectoryManager.listDirectory(path, this.out);
        }
        catch(BigFSException e)
        {
            writeError(e);
        }
        
    }
    
    private void getBigFSInodeInformation() throws IOException 
    {
        String file = in.readUTF();
        
        try
        {
            BigFSDirectoryManager.getInodeInformation(file, this.ugi, this.out);
        }
        catch(BigFSException e)
        {
            writeError(e);
        }        
    }
    
    
    private void createBigFSDirectory()  throws IOException
    {
        String directoryName = in.readUTF();

        try
        {
            BigFSDirectoryManager.createDirectory(directoryName, this.ugi);
            out.writeBoolean(true);
        }
        catch(BigFSException e)
        {
            writeError(e);
        }
    }
    
    
    /**
     * error codes:
     *    -100 "you can not open an already opened file"
     *    -101 "filename missing"
     *    -102 "you must close opened file before open another one" 
     * @throws IOException
     */
    public void createBigFSFile() throws IOException {        
        String fileName = in.readUTF();        
        int attributeLen = in.readInt();
        Map<String, String> fileAttributes = new HashMap<String, String>();
        
        for(int i=0; i<attributeLen;i++)
        {
            fileAttributes.put(in.readUTF(), in.readUTF());
        }
        
        try
        {
            BigFSDirectoryManager.createFile(fileName, fileAttributes, this.ugi);
            out.writeBoolean(true);
        }
        catch (BigFSException e)
        {   
            writeError(e);
        }
        
        
        
    }
    
    private void writeError(BigFSException e) throws IOException
    {
        e.printStackTrace();
        out.writeBoolean(false);
        out.writeInt(e.code().value);         
        out.writeUTF(e.getMessage());  
        out.flush();
    }
    
    private void close()
    {
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error closing socket", e);
        }
    }
}
