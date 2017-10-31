#!/usr/bin/env bash
pwd

host="192.168.13.133"

printf "rsync %s\n" $host

rsync -aurv \
 --exclude '.idea'       \
 --exclude 'backup'      \
 --exclude 'cmake-build-debug'  \
 --exclude 'nbproject'      \
 --exclude '.git'        \
 --exclude 'logs'        \
 --exclude 'data'        \
 --exclude '*.log*'        \
 --exclude '.gitignore'     \
 --exclude 'rsync-repo.sh'  \
 --exclude 'target'        \
 --exclude 'cmake'         \
 --progress              \
 ./ $host:/mnt/share133/kudu-examples-bench

 #--exclude '*.csv'        \