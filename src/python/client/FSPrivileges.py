class FSPrivileges(object):

    def __init__(self, user, group, permission):
      self.username = user;
      self.groupname = group;
      self.permission = permission;

    def getUserName(self):
        return self.username;

    def getGroupName(self):
        return self.groupname;

    def getPermission(self):
        return self.permission;

    def __str__(self):
      return self.username + ":" + self.groupname + ":" + str(self.permission);


if __name__ == '__main__':
    from FSPermission import FSPermission
    from FSAction import FSAction
    fp = FSPrivileges("timu", "timu", FSPermission(FSAction.READ_WRITE, FSAction.READ_WRITE, FSAction.EXECUTE))
    print fp
    print fp.getPermission().toShort();


"""

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException
    {
        username = in.readUTF();
        groupname = in.readUTF();
        permission = FSPermission.read(in);
    }

    /**
     * Create and initialize a {@link FSPrivileges} from {@link DataInput}.
     */
    public static FSPrivileges read(DataInput in) throws IOException
    {
        FSPrivileges p = new FSPrivileges();
        p.readFields(in);
        return p;
    }
}
"""

