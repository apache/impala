# VSCode Defaults

## Description
This folder contain default settings that provide a good set of defaults for using VSCode
to develop Impala.

* `cmake-kits.json`: Configures CMake to use the toolchain's C/C++ compiler.
* `launch.json`: Sets up debug launch configurations for Coordinator, Catalog, and JUnit
                 tests.
* `settings.json`: Main settings file that excludes unnecessary files from searching, the
                   explorer view, and Maven, includes for C++, and port forwarding during
                   remote development.

## How to Use

1. Copy the `.vscode-defaults` folder to `.vscode`.
2. Configure the default shell to automatically run `source bin/impala-config.sh` when a
   shell is launched.
3. Open the `${IMPALA_HOME}` folder in VSCode (do not use "Add Folder to Workspace").
   Note, if VSCode was opened before the default shell was configured to source the Impala
   config file, then VSCode will need to be reloaded for it to source the necessary
   environment variables.
4. Install VSCode Plugins:
    * C/C++ Extension Pack (extension id: ms-vscode.cpptools-extension-pack)
    * Extension Pack for Java (extension id: vscjava.vscode-java-pack)
    * Optional: Python (extension id: ms-python.python)
    * Optional: SQLTools (extension id: mtxr.sqltools)
    * Optional: SQLTools PostgreSQL/Cockroach Driver (extension id: mtxr.sqltools-driver-pg)

### Connecting to HMS DB

If the optional SQLTools and SQLTools PostgreSQL/Cockroach Driver extensions are
installed, then clicking on "SQLTools" in the left-hand Activity bar enables connecting to
the local HMS metastore db and exploring its structure/contents.

## Port Forwarding

During remote development (where a local VSCode GUI is connected to a VSCode server
running on a remote host), ports will automatically be forwarded from the local machine to
the remote machine as processes are started. Run the VSCode command
"Ports: Focus on Ports View" to see which ports are being forwarded. VSCode will attempt
to use the same port number (e.g. `localhost:28000` will forward to `remotehost:28000`)
unless the local port is already in use at which point VSCode will pick a random local
port.

## Debugging

When using the "(gdb) Attach to impalad" debug launch configuration, Impala must first
have been build without compiler optimizations using the command:
`./buildall.sh -skiptests -debug_noopt`
