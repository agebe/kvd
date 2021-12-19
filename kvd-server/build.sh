#!/bin/bash
set -e
docker pull gradle:jdk11
TAG=${1:-main}
TDIR=`mktemp -d`
echo $TDIR
function finish {
  rm -rf $TDIR
}
trap finish EXIT
cd $TDIR

git clone https://github.com/agebe/kvd.git

if [ $TAG = 'main' ]; then
  echo "build from main"
  DOCKER_TAG='latest'
else
  echo "build from $TAG"
  ( cd kvd && git checkout tags/$TAG )
  DOCKER_TAG=$TAG
fi
# make sure downloaded tools (e.g. formerly protoc) does not dirty the git workspace. All downloaded/extracted files need to be covered by .gitignore otherwise dirty shows up in version
docker run --rm -ti -u gradle --name "kvd-build" -v "$PWD/kvd":/home/gradle/project -w /home/gradle/project -e "GRADLE_USER_HOME=/home/gradle/project/.gradle" gradle:jdk11 bash -c "gradle dockerPrepare"
( cd kvd/kvd-server/build/docker && docker build --pull -t kvd:$DOCKER_TAG . )

echo done
