package org.bigfs.fs.file;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.exceptions.FileFullException;
import org.bigfs.fs.utils.Utils;

public class BigFSFile
{
    private String name;
    private String user;
    private List<Block> blocks;
    
    
    public BigFSFile(String name, String user, List<Block> blocks)  throws IOException
    {
        this.name = name;
        this.user = user;
        this.blocks = blocks;
    }
    

    
    public void addNewBlock(Block b)
    {
        this.blocks.add(b);
    }
    
    public List<Block> getBlocks()
    {
        return this.blocks;
    }
    
    public void write(DataInputStream in) 
    {
        while(true){
           Block lastBlock = blocks.get(blocks.size() - 1);
           if(lastBlock.size() >= BigFSConfiguration.getBlockSize())
           {
               this.addNewBlock(new Block(Utils.getTimeUUID().toString()));
               continue;
           }
           try {
               lastBlock.write(in);
           }
           catch(FileFullException ffe)
           {
               continue;
           }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       }
    }
}
