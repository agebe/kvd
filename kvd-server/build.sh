#!/bin/bash
set -e
docker pull gradle:jdk11
docker pull openjdk:16-buster
TAG=${1:-master}
TDIR=`mktemp -d`
echo $TDIR
function finish {
  rm -rf $TDIR
}
trap finish EXIT
cd $TDIR

git clone https://github.com/agebe/kvd.git

if [ $TAG = 'master' ]; then
  echo "build from master"
  DOCKER_TAG='latest'
else
  echo "build from $TAG"
  ( cd kvd && git checkout tags/$TAG )
  DOCKER_TAG=$TAG
fi

docker run --rm -ti -u gradle --name "kvd-build" -v "$PWD/kvd":/home/gradle/project -w /home/gradle/project -e "GRADLE_USER_HOME=/home/gradle/project/.gradle" gradle:jdk11 bash -c "wget https://github.com/protocolbuffers/protobuf/releases/download/v3.15.7/protoc-3.15.7-linux-x86_64.zip;unzip -qq protoc-3.15.7-linux-x86_64.zip;export PATH=bin:$PATH;gradle dockerPrepare"
( cd kvd/kvd-server/build/docker && docker build -t kvd:$DOCKER_TAG . )

echo done

