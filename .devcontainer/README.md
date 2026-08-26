# Apache Impala Devcontainer

## Setup
To develop on Apache Impala using a [devcontainer](https://containers.dev/), a pre-built container image is available on Docker Hub at [apache/impala-dev](https://hub.docker.com/r/apache/impala-dev).  To leverage this container image requires a machine with both Docker and the [devcontainer cli](https://github.com/devcontainers/cli?tab=readme-ov-file#npm-install) installed.

Once those prerequisites are met, pull the container image:
```
docker pull apache/impala-dev:stable
```

Then, retrieve the necessary config files:
```
git clone --single-branch --branch=master --filter=blob:none --sparse https://github.com/apache/impala.git
cd impala
git sparse-checkout set .devcontainer
```

Finally, connect an IDE to the machine and open the `impala` folder (in VSCode, the command is "Dev Containers: Open Folder in Container").  If prompted saying the devcontainer config file has changed should it be reloaded, select No.

## System Requirements
| Resource | Recommended | Minimum |
| -------- | ----------- | ------- |
| cpus     | 16          | 8       |
| memory   | 64gb        | 20gb    |
| storage  | 150gb       | 100gb   |

## Configuration
The Impala devcontainer is mostly configured and ready to go after startup.  The git `user.name`, `user.email`, and `core.editor` config settings are copied from the local machine into the devcontainer.

After the devcontainer is started, run the command `restart-minicluster` to spin up the necessary dependent services.

To start Impala itself, run `./bin/start-impala-cluster.py`.  Then connect to the cluster with `./bin/impala-shell.sh`.

## Timezone/Locale
The devcontainer is set up with the timezone and locale of the machine hosting the devcontainer.

To modify the timezone on a running container:
1. Update the `/etc/localtime` softlink to point to a different timezone directory.
2. Run `sudo dpkg-reconfigure tzdata`

To modify the locale (must be done before launching the devcontainer):
Edit `.devcontainer/devcontainer.json` updating the `runArgs` section adding the `LC_ALL` and `LANG` environment variables (no space after the `-e` flag):
```
  "runArgs": [
      "-eLANG=hu-HU.UTF-8",
      "-eLC_ALL=hu-HU.UTF-8",
      ...
  ]
```

## Cleanup
The Impala devcontainer leverages a docker volume which is not cleaned up when the devcontainer is destroyed.  Thus, the command `docker volume prune` must be run whenever a devcontainer is destroyed.  Once the docker volume is destroyed, all changes in the devcontainer are permanently deleted.

Happy developing!
