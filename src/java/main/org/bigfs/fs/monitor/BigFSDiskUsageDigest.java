package org.bigfs.fs.monitor;

import java.net.InetAddress;


public class BigFSDiskUsageDigest
{
    private long free_space = 0;  
    private double cpu_load = 0;
    
    private int last_check = 0;
    private int starting_time = 0; 
    
    final InetAddress node_address;
    
    public BigFSDiskUsageDigest(InetAddress node_address, long free_space, double cpu_load, int start_time, int last_check )
    {
        this.node_address =  node_address;
        this.free_space = free_space;
        this.last_check = last_check;
        this.starting_time = start_time;
        this.cpu_load = cpu_load;
    }
    
    public long getFreeSpace()
    {
        return free_space;
    }
    
    public int getLastCheckTime()
    {
        return last_check;
    }
    
    public int getStartingTime()
    {
        return this.starting_time;
    }
    
    public double getCpuLoad()
    {
        return cpu_load;
    }

    public InetAddress getNodeAddress()
    {
        return this.node_address;
    }
}
