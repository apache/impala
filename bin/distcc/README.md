# Distcc
Distcc will speed up compilation by distributing compilation tasks to remote build
machines. The scripts in this folder make using distcc easier.

# Requirements

The only requirement you should need to be aware of is, the scripts in this folder were
only tested on Linux. If you are using OS X, things probably won't work out of the box.

Assuming you are using Linux, if you use the scripts in this folder, there shouldn't be
any other requirements other than setting up your build farm and your BUILD_FARM variable.

Setting up a new distcc server is covered at the bottom of this document. Once your distcc
servers are configured, set the environment variable BUILD_FARM on your build machine to
to "host1/limit1,lzo host2/limit2,lzo" and so on.

The rest of the setup is done for you; here is a short description of what they do:

**You shouldn't need to do any of this, this scripts do this for you.**

1. Install distcc and ccache. Most Linux distros have these packages. The scripts will
   install it if you have a yum or apt-get based system. Otherwise you should install
   distcc and ccache yourself through whatever package manager your system uses.
1. Configure the remote distcc hosts.

# Usage

### First time
1. Symlink /opt/Impala-Toolchain to a directory writable by your user and point
  IMPALA_TOOLCHAIN at that directory. This ensures toolchain binaries are at the
  same path locally as on the distcc servers

        mkdir -p "$IMPALA_TOOLCHAIN"
        sudo ln -s "$IMPALA_TOOLCHAIN" /opt/Impala-Toolchain
        echo 'export IMPALA_TOOLCHAIN=/opt/Impala-Toolchain' >> bin/impala-config-local.sh

1. Source bin/impala-config.sh in the Impala repo. Step #2 depends on this.

        source "$IMPALA_HOME"/bin/impala-config.sh

1. Source "distcc_env.sh" in this directory. The script will attempt to install distcc
   if needed.

        source "$IMPALA_HOME"/bin/distcc/distcc_env.sh

1. Run buildall.sh. The main purpose is to regenerate cmakefiles.

        cd "$IMPALA_HOME"
        ./buildall.sh -skiptests -so   # Do not use -noclean

   You should notice that the build runs quite a bit faster.

### Incremental builds
At this point you no longer need to run the heavyweight buildall.sh. After editing files
you can either
```
make -j$(distcc -j)
```

### Switching back to local compilation
If you want to compile a very small change, a local build might be faster.
```
switch_compiler local
```
to switch back
```
switch_compiler distcc
```
### Second time
If you open a new terminal and attempt to build with "make" that will fail. To fix:
```
source "$IMPALA_HOME"/bin/impala-config.sh   # Skip if already done
source "$IMPALA_HOME"/bin/distcc/distcc_env.sh
```

# Setting up a new distcc server (automatic)
1. Run distcc_server_setup.sh to do initial setup for a server that accepts connections
   from the 172.\* private IP address block:
  sudo ./bin/distcc/distcc_server_setup.sh 172.16.0.0/12
1. Download toolchain packages. This is run initially and can be rerun to download
  new packages that a new version of Impala depends on.
  (. ./bin/impala-config.sh && ./infra/python/deps/download_requirements &&
   DOWNLOAD_CDH_COMPONENTS=false ./bin/bootstrap_toolchain.py)

# Setting up a new distcc server (manual)

1. Install "distccd" and "ccache".
1. Configure distccd (edit /etc/sysconfig/distccd on a RHEL server) with the options
   OPTIONS="--jobs 96 --allow YOUR.IP.ADDRESS.HERE --log-level=warn --nice=-15"
   Where num jobs = 2x the number of cores on the machine. (2x is recommended by distcc.)
1. Start distcc.
1. Edit distcc_env.sh to include the new host.
1. Install all required gcc, clang, binutils, etc, versions from the toolchain into
   /opt/Impala-Toolchain.
1. ccache stores its cache in $HOME/.ccache. Assuming distcc is running as a non-root user
   that has no $HOME, you must sudo mkdir /.ccache, then sudo chmod 777 /.ccache.
1. If distcc runs as "nobody", sudo -u nobody ccache -M 25G. This sets the size of the
   cache to 25GB. Adjust to your taste.

# Misc notes

1. "pump" doesn't work. Many compilation attempts time out say something like "Include
   server did not process the request in 3.8 seconds". distcc tries to copy 3rd party
   headers to the remote hosts and that may be the problem. If we could get the include
   server to use the remote 3rd party headers that should help.
1. Having a different local Linux OS on your development machine than on the distcc hosts
   should be fine.
