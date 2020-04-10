#!/bin/sh
echo Deploying devexec git stash
exec git pull origin dev
exec git checkout dev
if [ $(git rev-parse --abbrev-ref HEAD) = dev ]; then
   echo Successfully deployed dev!
else
   echo Failed to deployed dev.
fi
