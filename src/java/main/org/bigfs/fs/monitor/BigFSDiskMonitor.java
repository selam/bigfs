package org.bigfs.fs.monitor;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.bigfs.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.messages.BigFSDiskStatusMessage;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class BigFSDiskMonitor implements IEndpointStateChangeSubscriber
{
    final ConcurrentMap<InetAddress, BigFSDiskUsage> diskStateMap = new ConcurrentHashMap<InetAddress, BigFSDiskUsage>();
    
    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("DiskStatusTasks");
    
    private ScheduledFuture<?> scheduledBigFSDiskMonitor;
    
    public final static int intervalInMillis = 2000; // send information every 2 second
    
    public static final BigFSDiskMonitor instance = new BigFSDiskMonitor();
    
    private static final Logger logger = LoggerFactory.getLogger(BigFSDiskMonitor.class);
    
    private final Random random = new Random();
    
    private static OperatingSystemMXBean operatingSystem;
    private static File root_dir = new File(BigFSConfiguration.getDataDirectory());  
    static {
            operatingSystem = ManagementFactory.getOperatingSystemMXBean();        
    }
    
    private BigFSDiskMonitor()
    {
        diskStateMap.put(BigFSConfiguration.getListenAddress(), new BigFSDiskUsage());
        
        // in here i must be register my self to gossip service for endpoint state changes        
        Gossiper.instance.register(this);
    }
    
    private class BigFSDiskMonitorTask implements Runnable
    {
        public void run()
        {
            try
            {   
                /* Update the local disk state */
                diskStateMap.get(BigFSConfiguration.getListenAddress()).updateDiskUsage(
                        root_dir.getFreeSpace());
                diskStateMap.get(BigFSConfiguration.getListenAddress()).updateCpuLoad(
                        operatingSystem.getSystemLoadAverage()    
                );
                
                // send all disk information we have to some live members                 
                List<BigFSDiskUsageDigest> usages = BigFSDiskMonitor.instance.diskUsages();
                
                /*
                 * send our disk usage to other some nodes for knowlage, 
                 * they send to they list to for our knowlage as return
                 **/
                MessageOut<BigFSDiskStatusMessage> message = new MessageOut<BigFSDiskStatusMessage>(
                        BigFSDiskStatusMessage.messageGroup,
                        new BigFSDiskStatusMessage(usages),
                        BigFSDiskStatusMessage.serializer
                );
                
                List<InetAddress> liveMembers = Lists.newArrayList(Gossiper.instance.getLiveMembers());
                
                Collections.shuffle(liveMembers, random);
                
                InetAddress sendTo = liveMembers.get(0);
                
                MessagingService.instance().sendOneWay(message, sendTo);
                logger.info("We send our disk status to {}", sendTo);                
            }
            catch (Exception e)
            {
                logger.error("BigFSDiskMonitor error", e);
            }
        }
    }
    
    private List<BigFSDiskUsageDigest> diskUsages()
    {
        BigFSDiskUsage usage;
        
        List<BigFSDiskUsageDigest> diskUsageDigests = new ArrayList<BigFSDiskUsageDigest>();
        List<InetAddress> endpoints = new ArrayList<InetAddress>(diskStateMap.keySet());
        Collections.shuffle(endpoints, random);
        for (InetAddress endpoint : endpoints)
        {
            usage = diskStateMap.get(endpoint);
            diskUsageDigests.add(new BigFSDiskUsageDigest(endpoint, usage.getFreeSpace(), 
                                                                    usage.getCpuLoad(), 
                                                                    usage.getStartingTime(), 
                                                                    usage.getLastCheckTime()));
            
        }

        return diskUsageDigests;
    }    
    
    
    
    /**
     * Start the disk monitor
     */
    public void start()
    {
        scheduledBigFSDiskMonitor = executor.scheduleWithFixedDelay(new BigFSDiskMonitorTask(),
                                                              BigFSDiskMonitor.intervalInMillis,
                                                              BigFSDiskMonitor.intervalInMillis,
                                                              TimeUnit.MILLISECONDS);
    }
    
    public void stop()
    {
        scheduledBigFSDiskMonitor.cancel(false);
        logger.info("disk status shutdown");
        try
        {
            Thread.sleep(intervalInMillis);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }        
    }

    @Override
    public void onAlive(InetAddress address, EndpointState arg1)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onChange(InetAddress address, ApplicationState arg1,
            VersionedValue arg2)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onDead(InetAddress address, EndpointState arg1)
    {       
        diskStateMap.remove(address);
    }

    @Override
    public void onJoin(InetAddress address, EndpointState arg1)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onRemove(InetAddress address)
    {
        diskStateMap.remove(address);        
    }

    @Override
    public void onRestart(InetAddress address, EndpointState arg1)
    {
        // TODO Auto-generated method stub
        
    }

    private void updateDiskUsageIstatistics(BigFSDiskUsageDigest digest)
    {
        diskStateMap.get(digest.getNodeAddress()).updateDiskUsage(digest.getFreeSpace());
        diskStateMap.get(digest.getNodeAddress()).updateCpuLoad(digest.getCpuLoad());
        diskStateMap.get(digest.getNodeAddress()).updateStartingTime(digest.getStartingTime());
        diskStateMap.get(digest.getNodeAddress()).updateLastCheckTime(digest.getLastCheckTime());        
    }
    
   
    private void addListForSendToRemote(List<BigFSDiskUsageDigest>list, InetAddress address, BigFSDiskUsage usage)
    {
        list.add(
            new BigFSDiskUsageDigest(
                    address,
                    usage.getFreeSpace(),
                    usage.getCpuLoad(),
                    usage.getStartingTime(),
                    usage.getStartingTime()
            ) 
        );
    }
    
    
    public void examineDiskUsage(List<BigFSDiskUsageDigest> diskUsageDigests, List<BigFSDiskUsageDigest> local_usages)
    {  
       //not in your list
       List<InetAddress> not_in_remote = new ArrayList<InetAddress>();       
       for(BigFSDiskUsageDigest digest: diskUsageDigests)
       {
           not_in_remote.add(digest.getNodeAddress());
           
           BigFSDiskUsage usage = diskStateMap.get(digest.getNodeAddress());
           if(usage != null)
           {
               if(usage.getStartingTime() < digest.getStartingTime())
               {
                   // then update our information from remote information     
                   this.updateDiskUsageIstatistics(digest);
               }               
               else if(usage.getStartingTime() > digest.getStartingTime())
               {
                   // then send our information to remote
                   addListForSendToRemote(local_usages, digest.getNodeAddress(), usage);
               }
               else if(usage.getStartingTime() == digest.getStartingTime())
               {
                   if(usage.getLastCheckTime() < digest.getLastCheckTime())
                   {
                       this.updateDiskUsageIstatistics(digest);
                   }
                   else if(usage.getLastCheckTime() > digest.getLastCheckTime())
                   {
                       addListForSendToRemote(local_usages, digest.getNodeAddress(), usage);
                   }
               }
               else if(usage.getStartingTime() == digest.getStartingTime() && usage.getLastCheckTime() == digest.getLastCheckTime()) 
               {
                   // no operation
                   continue;
               }         
           }
           else
           {
               diskStateMap.putIfAbsent(digest.getNodeAddress(), new BigFSDiskUsage(                       
                           digest.getFreeSpace(),
                           digest.getCpuLoad(),
                           digest.getStartingTime(),
                           digest.getStartingTime()
                   )
               );               
           } 
           // uzak noktadaki node üzerinde bulunmayan tüm bilgileri gönder.
           Set<InetAddress> all_nodes = diskStateMap.keySet();           
           all_nodes.removeAll(not_in_remote);
           if(!all_nodes.isEmpty())
           {
               for(InetAddress address: all_nodes)
               {
                   addListForSendToRemote(local_usages, address, diskStateMap.get(address));
               }
           }
       }
    }
}