#!/bin/bash
set -e
docker pull gradle:jdk8
docker pull openjdk:15-buster
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

docker run --rm -ti -u gradle --name "kvd-build" -v "$PWD/kvd":/home/gradle/project -w /home/gradle/project -e "GRADLE_USER_HOME=/home/gradle/project/.gradle" gradle:jdk8 gradle dockerPrepare
( cd kvd/kvd-server/build/docker && docker build -t kvd:$DOCKER_TAG . )

echo done

