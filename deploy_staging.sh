#!/bin/sh
echo Deploying stagingexec git stash
exec git pull origin staging
exec git checkout staging
if [ $(git rev-parse --abbrev-ref HEAD) = staging ]; then
   echo Successfully deployed staging!
else
   echo Failed to deployed staging.
fi
