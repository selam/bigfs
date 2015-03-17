#!/usr/bin/env python

ACTIONS = [
            ('NONE','---'),
            ('EXECUTE','--x'),
            ('WRITE','-w-")'),
            ('WRITE_EXECUTE','-wx'),
            ('READ','r--'),
            ('READ_EXECUTE','r-x'),
            ('READ_WRITE','rw-'),
            ('ALL', 'rwx')
        ]

VALUES = [i for i in xrange(0, len(ACTIONS))]

class FSAction(object):
  class __metaclass__(type):
        def __getattr__(self, name):
            for _s in ACTIONS:
              if _s[0] == name:
                  return FSAction(ACTIONS.index(_s));
            raise Exception("Not found")


  def ordinal(self):
      return self.SYMBOL

  def __init__(self, s):
    self.SYMBOL = s

  def implies(self, that):
    if (that != None):
      return (self.ordinal() & that.ordinal()) == that.ordinal();
    return False;


  def __eq__(self, that):
      return self.ordinal() == that.ordinal()

  # and
  def And(self, that):
    return FSAction(VALUES[self.ordinal() & that.ordinal()]);

  # Or
  def Or(self, that):
    return FSAction(VALUES[self.ordinal() | that.ordinal()]);

  # not
  def Not(self):
    return FSAction(VALUES[7 - self.ordinal()]);

  def __str__(self):
      return ACTIONS[self.SYMBOL][1]

  @staticmethod
  def values():
      return VALUES;

if __name__ == '__main__':
    a = FSAction.NONE
    b = FSAction.READ_WRITE
    print a.And(b)
    print a.Or(b)
    print a.Not()