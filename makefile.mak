
#
# define compiler and compiler flag variables
#

JFLAGS = -g
JC = javac

.SUFFIXES: .java .class

.java.class:
        $(JC) $(JFLAGS) $*.java

CLASSES = \
        ControlClient.java \
        ControlClientThread.java \
        DataNode.java \
		DataNodeThread.java \
		RegularClient.java \
        RegularClientThread.java \
		DHTRingNode.java \
		Hashing.java \
		MurMurHashing.java \
		ReadPropFile.java

#
# the default make target entry
#

default: classes

classes: $(CLASSES:.java=.class)

clean:
        $(RM) *.class