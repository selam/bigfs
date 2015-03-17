package org.bigfs.fs.file;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.exceptions.FileFullException;
import org.bigfs.fs.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Block
{
    private static final Logger logger = LoggerFactory.getLogger(Block.class);
    
    private final String name;
    private int size = 0;
    private final List<InetAddress> locations;
    
    private boolean on_this_node = false;
    
    public Block(String name)
    {
        this.name = name;
        this.locations = Utils.getKeyLocations(ByteBufferUtil.bytes(this.name));        
        on_this_node = Utils.inThatNode(this.locations);
        try 
        {
            checkAndCreate();
        }
        catch(IOException e) {
            
        }
    }
    
    private void checkAndCreate() throws IOException
    {
        if(on_this_node)
        {
           File f = new File(BigFSConfiguration.getDataDirectory()+"/"+this.getBlockPath());
           f.getParentFile().mkdirs();
           f.createNewFile();
        }
    }
    
    public String getBlockPath()
    {
        return String.format("%s/%s/%s", name.substring(0, 2), name.substring(2, 4), name);
    }
    
    public List<InetAddress> getLocations() 
    {
        return this.locations;
    }
    
    public int size()
    {
        return this.size;
    }
    
    public void write(DataInputStream in) throws IOException, FileFullException
    {
        logger.info("block write?");
        if(on_this_node)
        {
            logger.info("block write??");
            File f = new File(BigFSConfiguration.getDataDirectory()+"/"+this.getBlockPath());
            FileOutputStream fos = new FileOutputStream(f);
            byte[] buffer = new byte[1024 * 1024];
            int read = 0;
            int readed = 0;
            try 
            {   while ((read = in.read(buffer)) != -1) {
                  fos.write(buffer, 0, read);
                  readed += read;
                  if(readed >= BigFSConfiguration.getBlockSize())
                  {
                      throw new FileFullException();                  
                  }
                }    
            }
            catch(FileFullException e)
            {
              throw e;   
            }
            finally 
            {
                fos.close();
            }
        }        
    }
}
