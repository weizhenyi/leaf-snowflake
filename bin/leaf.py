#!/usr/bin/python

import os
import sys
import random
import subprocess as sub
import getopt

def identity(x):
    return x

def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = sub.Popen(command,stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    return lines[0]

if sys.platform == "cygwin":
    normclasspath = cygpath
else:
    normclasspath = identity

CLIENT_CONF_FILE = ""
LEAF_DIR = "/".join(os.path.realpath( __file__ ).split("/")[:-2])
LEAF_CONF_DIR = os.getenv("LEAF_CONF_DIR", LEAF_DIR + "/conf" )
LOGBACK_CONF = LEAF_CONF_DIR + "/leaf.logback.xml"
CONFIG_OPTS = []
EXCLUDE_JARS = []
INCLUDE_JARS = []
STATUS = 0


def check_java():
    check_java_cmd = 'which java'
    ret = os.system(check_java_cmd)
    if ret != 0:
        print("Failed to find java, please add java to PATH")
        sys.exit(-1)

def get_config_opts():
    global CONFIG_OPTS
    return "-Dleaf.options=" + (','.join(CONFIG_OPTS)).replace(' ', "%%%%")

def get_client_childopts():
    ret = (" -Dleaf.root.logger=INFO,stdout " +
           " -Dlog4j.configuration=File:" + LEAF_DIR +
           "/conf/client_log4j.properties")
    if CLIENT_CONF_FILE != "":
        ret += (" -Dleaf.conf.file=" + CLIENT_CONF_FILE)
    return ret

def get_server_childopts(log_name):
    leaf_log_dir = get_log_dir()
    gc_log_path = leaf_log_dir + "/" + log_name + ".gc"
    ret = (" -Xloggc:%s -Dlogfile.name=%s -Dlogback.configurationFile=%s -Dleaf.log.dir=%s "  %(gc_log_path, log_name, LOGBACK_CONF, leaf_log_dir))
    return ret

if not os.path.exists(LEAF_DIR + "/RELEASE"):
    print "******************************************"
    print "The leaf client can only be run from within a release. You appear to be trying to run the client from a checkout of leaf's source code."
    print "\nYou can download a leaf release "
    print "******************************************"
    sys.exit(1)

def get_jars_full(adir):
    files = os.listdir(adir)
    ret = []
    for f in files:
        if f.endswith(".jar") == False:
            continue
        filter = False
        for exclude_jar in EXCLUDE_JARS:
            if f.find(exclude_jar) >= 0:
                filter = True
                break

        if filter == True:
            print "Don't add " + f + " to classpath"
        else:
            ret.append(adir + "/" + f)
    return ret

def get_classpath(extrajars):
    ret = []
    ret.extend(extrajars)
    ret.extend(get_jars_full(LEAF_DIR))
    ret.extend(get_jars_full(LEAF_DIR + "/lib"))
    ret.extend(INCLUDE_JARS)
    return normclasspath(":".join(ret))


def confvalue(name, extrapaths):
    command = [
        "java", "-client", "-Xms256m", "-Xmx256m", get_config_opts(), "-cp", get_classpath(extrapaths), "Config.config_value", name
    ]
    print command
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    print "Failed to get config " + name
    print errors
    print output

def print_localconfvalue(name):
    """Syntax: [leaf localconfvalue conf-name]
    """
    print name + ": " + confvalue(name, [LEAF_CONF_DIR])

def get_log_dir():
    cppaths = [LEAF_CONF_DIR]
    leaf_log_dir = confvalue("leaf.log.dir", cppaths)
    if not leaf_log_dir == "null":
       if not os.path.exists(leaf_log_dir):
          os.mkdir(leaf_log_dir)
    else:
       leaf_log_dir = LEAF_DIR + "/logs"
       if not os.path.exists(leaf_log_dir):
          os.mkdir(leaf_log_dir)
    return leaf_log_dir

def print_remoteconfvalue(name):
    """Syntax: [leaf remoteconfvalue conf-name]
    """
    print name + ": " + confvalue(name, [LEAF_CONF_DIR])


def exec_leaf_class(klass, jvmtype="-server", childopts="", extrajars=[], args=[]):
    nativepath = confvalue("java.library.path", extrajars)
    args_str = " ".join(map(lambda s: "\"" + s + "\"", args))
    print args_str
    if "leaf" in klass:
        # fix cmd > 4096, use dir in cp, only for leaf server
        command = "java " + jvmtype + " -Dleaf.home=" + LEAF_DIR + " " + get_config_opts() + " -Djava.library.path=" + nativepath + " " + childopts + " -cp " + get_classpath(extrajars) + ":" + LEAF_DIR + "/lib/* " + klass + " " + args_str
    else:
        command = "java " + jvmtype + " -Dleaf.home=" + LEAF_DIR + " " + get_config_opts() + " -Djava.library.path=" + nativepath + " " + childopts + " -cp " + get_classpath(extrajars) + " " + klass + " " + args_str
    print "Running: " + command
    global STATUS
    STATUS = os.system(command)


def zktool(*args):
    childopts = get_client_childopts()
    exec_leaf_class(
        "zookeeper.ZkTool",
        jvmtype="-client -Xms256m -Xmx256m",
        extrajars=[ LEAF_CONF_DIR, CLIENT_CONF_FILE],
        args=args,
        childopts=childopts)







def start():
    """Syntax: [leaf start]
    """
    cppaths = [LEAF_CONF_DIR]
    leaf_classpath = confvalue("leaf.classpath", cppaths)
    childopts = confvalue("leaf.childopts", cppaths) + get_server_childopts("leaf.log")
    exec_leaf_class(
        "leaf.leafServer",
        jvmtype="-server",
        extrajars=(cppaths+[leaf_classpath]),
        childopts=childopts)

def stop():
    """Syntax: [leaf stop]
        use /bin/stop.sh instead
    """
    pass




def print_classpath():
    """Syntax: [leaf classpath]

    Prints the classpath used by the jstorm client when running commands.
    """
    print get_classpath([])

def print_commands():
    """Print all client commands and link to documentation"""
    print "leaf command [--config client_leaf.yaml] [--exclude-jars exclude1.jar,exclude2.jar] [-c key1=value1,key2=value2][command parameter]"
    print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
    print "\n\t[--config client_leaf.yaml]\t\t\t optional, setting client's leaf.yaml"
    print "\n\t[--exclude-jars exclude1.jar,exclude2.jar]\t optional, exclude jars, avoid jar conflict"
    print "\n\t[-c key1=value1,key2=value2]\t\t\t optional, add key=value pair to configuration"
    print "\nHelp:", "\n\thelp", "\n\thelp <command>"

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if COMMANDS.has_key(command):
            print (COMMANDS[command].__doc__ or
                  "No documentation provided for <%s>" % command)
        else:
           print "<%s> is not a valid command" % command
    else:
        print_commands()

def unknown_command(*args):
    print "Unknown command: [leaf %s]" % ' '.join(sys.argv[1:])
    print_usage()



COMMANDS = {"start": start , "zktool": zktool, "localconfvalue": print_localconfvalue,
            "remoteconfvalue": print_remoteconfvalue, "classpath": print_classpath, "help": print_usage,}

def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)

def parse_exclude_jars(jars):
    global EXCLUDE_JARS
    EXCLUDE_JARS = jars.split(",")
    print " Excludes jars:"
    print EXCLUDE_JARS

def parse_include_jars(jars):
    global INCLUDE_JARS
    INCLUDE_JARS = jars.split(",")
    print " Include jars:"
    print INCLUDE_JARS

def parse_config_opts(args):
  curr = args[:]
  curr.reverse()
  config_list = []
  args_list = []

  while len(curr) > 0:
    token = curr.pop()
    if token == "-c":
      config_list.append(curr.pop())
    elif token == "--config":
      global CLIENT_CONF_FILE
      CLIENT_CONF_FILE = curr.pop()
    elif token == "--exclude-jars":
      parse_exclude_jars(curr.pop())
    elif token == "--include-jars":
      parse_include_jars(curr.pop())
    else:
      args_list.append(token)

  return config_list, args_list

def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    global CONFIG_OPTS
    config_list, args = parse_config_opts(sys.argv[1:])
    parse_config(config_list)
    COMMAND = args[0]
    ARGS = args[1:]
    if COMMANDS.get(COMMAND) == None:
        unknown_command(COMMAND)
        sys.exit(-1)
    if len(ARGS) != 0 and ARGS[0] == "help":
        print_usage(COMMAND)
        sys.exit(0)
    try:
        (COMMANDS.get(COMMAND, "help"))(*ARGS)
    except Exception, msg:
        print(msg)
        print_usage(COMMAND)
        sys.exit(-1)
    sys.exit(STATUS)

if __name__ == "__main__":
    check_java()
    main()

