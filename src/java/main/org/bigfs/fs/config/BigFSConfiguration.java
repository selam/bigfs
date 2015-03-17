package org.bigfs.fs.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

public class BigFSConfiguration
{
    
    private static final Logger logger = LoggerFactory.getLogger(BigFSConfiguration.class);
    
    private static BigFSConfiguration configuration;
    
    private final static String DEFAULT_CONFIGURATION = "bigfs.yaml";
    public String data_dir = "/var/lib/bigfs/data";
    public String listen_address;
    
    public int listen_port = 8080;
    
    public String KEYSPACE = "BigFS";
    public String COLUMN_FAMILY = "Files";
    
    public int block_size = 1073741888;
    public int default_file_permission = 644;
    
    public int default_directory_permission = 755;
    
    public int rpc_port = 1977;
    
    public String read_consistency; 
    
    public String write_consistency;
    
    private static Map<String, ConsistencyLevel> CONSISTENCIES = new HashMap<String, ConsistencyLevel>();
    
    static URL getStorageConfigURL() throws RuntimeException
    {
        String configUrl = System.getProperty("bigfs.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

        URL url;
        try
        {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        }
        catch (Exception e)
        {
            ClassLoader loader = BigFSConfiguration.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null)
                throw new RuntimeException("Cannot locate " + configUrl);
        }

        return url;
    }
    
    static {
        
        CONSISTENCIES.put("ANY", ConsistencyLevel.ANY);
        CONSISTENCIES.put("ONE", ConsistencyLevel.ONE);
        CONSISTENCIES.put("TWO", ConsistencyLevel.TWO);
        CONSISTENCIES.put("THREE", ConsistencyLevel.THREE);
        CONSISTENCIES.put("QUORUM", ConsistencyLevel.QUORUM);
        CONSISTENCIES.put("ALL", ConsistencyLevel.ALL);
        CONSISTENCIES.put("LOCAL_QUORUM", ConsistencyLevel.LOCAL_QUORUM);
        CONSISTENCIES.put("EACH_QUORUM", ConsistencyLevel.EACH_QUORUM);
        
        loadYaml();
    }
    
    static void loadYaml()
    {
        
        try
        {
            URL url = getStorageConfigURL();
            logger.info("Loading settings from " + url);
            InputStream input;
            try
            {
                input = url.openStream();
            }
            catch (IOException e)
            {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(BigFSConfiguration.class);
            Yaml yaml = new Yaml(new Loader(constructor));
            configuration = (BigFSConfiguration)yaml.load(input);

            if (!System.getProperty("os.arch").contains("64"))
                logger.info("32bit JVM detected. It is recommended to run BigFS on a 64bit JVM for better performance.");

            if(configuration.data_dir == null)
            {
               throw new RuntimeException("data_dir directory missing");               
            }
            
            if(configuration.data_dir.endsWith("/")){
                int last_slash = configuration.data_dir.lastIndexOf("/");
                
                configuration.data_dir = configuration.data_dir.substring(0, last_slash);
             }
            
             FileUtils.createDirectory(configuration.data_dir);
            
        }            
        catch (YAMLException e)
        {
            logger.error("Fatal configuration error error", e);
            System.err.println(e.getMessage() + "\nInvalid yaml; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }
        catch(RuntimeException e)
        {
            logger.error("Fatal configuration error error", e);
            System.err.println(e.getMessage() + "\n unable to start server.  See log for stacktrace.");
            System.exit(1);            
        }
    }
    
    
    public static InetAddress getListenAddress()
    {
        try
        {
            return InetAddress.getByName(configuration.listen_address);
        }
        catch (UnknownHostException e)
        {
           return FBUtilities.getLocalAddress();
        }
    }
    
    public static int getListenPort()
    {
        return configuration.listen_port;
    }
    
    public static String getKeyspace()
    {
        return configuration.KEYSPACE;
    }
    
    public static String getColumnFamily()
    {
        return configuration.COLUMN_FAMILY;
    }
    
    public static String getDataDirectory()
    {   
        return configuration.data_dir;
    }
    
    public static int getBlockSize()
    {   
        return configuration.block_size;
    }


    public static Object getDefaultFilePermission()
    {
        return configuration.default_file_permission;
    }


    public static Object getDefaultDirectoryPermission()
    {
        return configuration.default_directory_permission;
    }
    
    public static int getRPCPort() 
    {
        return configuration.rpc_port;
    }
    
    public static ConsistencyLevel getReadConsistencyLevel()
    {
        return CONSISTENCIES.get(configuration.read_consistency);
    }
    
    public static ConsistencyLevel getWriteConsistencyLevel()
    {
        return CONSISTENCIES.get(configuration.write_consistency);
    }   
}
