package org.bigfs.fs.monitor;


public class BigFSDiskUsage
{
    
    private long free_space = 0;  
    private double cpu_load = 0;
    
    private int last_check = 0;
    private int starting_time = 0; 
    
    
    /**
     * 
     * @param free_space 
     * @param cpu_load
     * @param starting_time
     * @param last_check
     */
    
    public BigFSDiskUsage(long free_space, double cpu_load, int starting_time, int last_check)
    {
        this.free_space = free_space; 
        this.last_check = last_check;  
        this.starting_time = starting_time;
        this.cpu_load = cpu_load;
    }
    
    public BigFSDiskUsage()
    {
        this.starting_time = (int) System.currentTimeMillis() / 1000;
        this.last_check = (int) System.currentTimeMillis() / 1000;
        this.free_space = 0;
        this.cpu_load = 100;
    }
    
    public void updateDiskUsage(long free_space)
    {
       this.free_space = free_space;
    }
    
    public void updateLastCheckTime(int last_check)
    {
       this.last_check = last_check;        
    }
    
    public void updateStartingTime(int start_time)
    {
       this.starting_time = start_time;        
    }

    public void updateCpuLoad(double systemLoadAverage)
    {
        this.cpu_load = systemLoadAverage;        
    }
    
    public long getFreeSpace()
    {
        return this.free_space;
    }
    
    public int getLastCheckTime()
    {
        return this.last_check;
    }

    public int getStartingTime()
    {
        return this.starting_time;
    }    
    public double getCpuLoad()
    {
        return cpu_load;
    }
}
