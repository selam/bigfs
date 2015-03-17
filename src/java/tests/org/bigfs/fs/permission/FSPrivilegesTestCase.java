package org.bigfs.fs.permission;

/**
 * User: timu
 * Date: 3/29/13
 * Time: 6:20 PM
 * To change this template use File | Settings | File Templates.
 */

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.bigfs.fs.security.UserGroupInformation;


public class FSPrivilegesTestCase extends TestCase {

  FSPrivileges privileges;

  public void setUp()
  {
      privileges = new FSPrivileges("test-user", "test-group", new FSPermission(700));
  }

  public void testCanWrite()
  {
     assertTrue("user can write", privileges.canDo(new UserGroupInformation("test-user", "test-user"), FSAction.WRITE_EXECUTE));
     assertFalse("user can not write but group can", privileges.canDo(new UserGroupInformation("test-user-2", "test-group"), FSAction.WRITE_EXECUTE));
     assertFalse("user can not write", privileges.canDo(new UserGroupInformation("test-user-2", "test-group-2"), FSAction.WRITE_EXECUTE));
  }

  public void tearDown()
  {
    // Teardown for data used by the unit tests
  }

   public static TestSuite suite() {
		return new TestSuite(FSPrivilegesTestCase.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }
}
