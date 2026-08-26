# Building the Apache Impala Devcontainer Image

Because of the lengthy process for the initial dev machine setup, a pre-built container
image is used. This folder contains all the tools necessary to create that image.

## Prerequisites
The build machine must have git, docker, and the devcontainer cli installed.

### Ubuntu
To install prereqs on Ubuntu, run the following commands as root.
```
apt update
apt install -y apt-transport-https ca-certificates curl software-properties-common nodejs npm bash-completion jq
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list
apt update
apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
npm install -g @devcontainers/cli
```

## Step: Build and Push Container Image
Note: For best results, build separate images on separate x86_64 and ARM machines.
1. Retrieve the latest code: `git clone --single-branch --branch=master https://github.com/apache/impala.git`
2. `docker login` with credentials that can push to the `apache/impala-dev` repo on Docker Hub.
3. Build and push image under the `latest-{{PLATFORM}}` tag: `PUSH=1 ./.devcontainer-build/build-devcontainer.sh 2>&1 | tee ~/build-output.log`
4. Once both x86 and ARM images are built, create multi-platform manifest under the `latest` tag: `./.devcontainer-build/setup-multiplatform-manifest.sh`

## Step: Promote Images to stable
Once the newly created images have been tested, promote them to stable.
1. Move existing `stable` tag to `stable-prev`: `OCI_SRC_TAG=stable OCI_DEST_TAG=stable-prev ./.devcontainer-build/tag-multiplatform-manifest.sh`
2. Move existing `latest` tag to `stable`: `OCI_SRC_TAG=latest OCI_DEST_TAG=stable ./.devcontainer-build/tag-multiplatform-manifest.sh`

## Step: Restore Previous Stable Image
If there is an issue with the `stable` image, and a backout is neccessary, run: `OCI_SRC_TAG=stable-prev OCI_DEST_TAG=stable ./.devcontainer-build/tag-multiplatform-manifest.sh`
