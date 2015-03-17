/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bigfs.fs.service;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.service.CassandraDaemon;
import org.bigfs.fs.config.BigFSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigFSDaemon extends CassandraDaemon implements BigFSDaemonMBean {
	
	private static final Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);
	
	private static final BigFSDaemon instance = new BigFSDaemon();
	
	public BigFSServer bigFSServer;

	public static final String MBEAN_NAME = "org.bigfs.fs.service:type=Daemon";
	
    private BigFSDaemon()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public void startCassandraThriftInterface()
	{
	    super.start();     
	}
    
    public void start()
    {
        
        logger.info("BigFS starting...");
        
		bigFSServer.start();	
		startCassandraThriftInterface();
    }
	
      
    public void stop()
    {
        logger.info("BigFS shutting down...");
        if(bigFSServer != null)
        {
            bigFSServer.stop();
        }
    }
    
    protected void setup() 
    {        
        super.setup();
        try 
        {
            bigFSServer = new BigFSServer(BigFSConfiguration.getListenAddress(), BigFSConfiguration.getListenPort());
        }
        catch(Exception ex)
        {
           logger.info("BigFSServer throws an excetion {}, exiting", ex.getMessage());
           System.exit(-5);
        }
    }
    
    public static void main(String[] args)
    {
        instance.activate();
    }
    
    public void activate()
    {
        String pidFile = System.getProperty("bigfs-pidfile");
        
        try
        {
            setup();
            
            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }
            
            if (System.getProperty("bigfs-foreground") == null)
            {
               // System.out.close();
               // System.err.close();
            }
            
            start();
        }
        catch (Throwable e)
        {
            logger.error("Exception encountered during startup", e);

            // try to warn user on stdout too, if we haven't already detached
            e.printStackTrace();
            System.out.println("Exception encountered during startup: " + e.getMessage());

            System.exit(3);
        }
    }
}
