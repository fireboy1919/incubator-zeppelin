/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.resource;

import com.google.gson.Gson;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventPoller;
import org.apache.zeppelin.interpreter.remote.mock.MockInterpreterResourcePool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unittest for DistributedResourcePool
 */
public class VFSResourcePoolTest {
  private HashMap<String, String> env;
  private Properties props;
  private VFSResourcePool pool;
  private ResourcePoolConnector mockConnector;
  
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Before
  public void setUp() throws Exception {
    env = new HashMap<String, String>();
    env.put("ZEPPELIN_CLASSPATH", new File("./target/test-classes").getAbsolutePath());
    props = new Properties();
    props.setProperty("ResourcePoolClass", "org.apache.zeppelin.resource.VFSResourcePool");
    props.setProperty("Resource_Path", testFolder.newFolder().getAbsolutePath());
    
    mockConnector = new ResourcePoolConnector() {
      @Override
      public ResourceSet getAllResources() {
        ResourceSet set = new ResourceSet();

        ResourceSet remoteSet = new ResourceSet();
        Gson gson = new Gson();
        for (Resource s : set) {
          RemoteResource remoteResource = gson.fromJson(gson.toJson(s), RemoteResource.class);
          remoteResource.setResourcePoolConnector(this);
        }
        return remoteSet;
      }

      @Override
      public Object readResource(ResourceId id) {
        return null;
      }
    };
    
    pool = new VFSResourcePool("pool1", mockConnector, props);


  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSingle() {
    pool.put("test", "Test");
    assertEquals(pool.get("test").get(), "Test");
  }

  @Test
  public void testGetAll() {
    pool.put("test", "Test");
    pool.put("test2", "Test2");
    assertEquals(pool.getAll().size(), 2);
  }
  
  @Test
  public void testDelete() {
    pool.put("test", "Test");
    pool.remove("test");
    assertEquals(pool.get("test"), null);
  }
  
  @Test
  public void testPersist() {
    pool.put("test", "Test");
    VFSResourcePool pool2 = new VFSResourcePool("test2", mockConnector, props);
    assertEquals(pool2.get("test").get(), "Test");
  }


}
