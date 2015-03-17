package org.bigfs.fs.utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.bigfs.fs.config.BigFSConfiguration;

public class Utils
{
    private static final Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();
    

    public static List<InetAddress> getKeyLocations(ByteBuffer key)
    {
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(Table.open(BigFSConfiguration.getKeyspace()), key);
        // do not sort by proximity,
        // we must sort ip's by ip addresses 
        //DatabaseDescriptor.getEndpointSnitch().sortByProximity(BigFSConfiguration.getListenAddress(), endpoints);
        Collections.sort(endpoints, new Comparator<InetAddress>()
        {

            @Override
            public int compare(InetAddress adr1, InetAddress adr2)
            {
                byte[] ba1 = adr1.getAddress();
                byte[] ba2 = adr2.getAddress();
         
                // general ordering: ipv4 before ipv6
                if(ba1.length < ba2.length) return -1;
                if(ba1.length > ba2.length) return 1;
         
                // we have 2 ips of the same type, so we have to compare each byte
                for(int i = 0; i < ba1.length; i++) {
                    int b1 = unsignedByteToInt(ba1[i]);
                    int b2 = unsignedByteToInt(ba2[i]);
                    if(b1 == b2)
                        continue;
                    if(b1 < b2)
                        return -1;
                    else
                        return 1;
                }
                return 0;
            }
            
            private int unsignedByteToInt(byte b) {
                return (int) b & 0xFF;
            }            
        });
        
       return endpoints;
    }
    
    public static List<InetAddress> getKeyLocations(String key)
    {
        return Utils.getKeyLocations(ByteBufferUtil.bytes(key));
    }
    
    
    public static boolean inThatNode(List<InetAddress> nodelist)
    {
        for(InetAddress address: nodelist)
        {
            if(localAddresses.contains(address)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Gets a new time uuid.
     *
     * @return the time uuid
     */
    public static java.util.UUID getTimeUUID()
    {
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }
}
