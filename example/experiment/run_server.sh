#!/bin/bash

# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../shflags

# define command-line flags
DEFINE_string crash_on_fatal 'true' 'Crash on fatal log'
DEFINE_integer bthread_concurrency '18' 'Number of worker pthreads'
DEFINE_string sync 'true' 'fsync each time'
DEFINE_string valgrind 'false' 'Run in valgrind'
DEFINE_integer max_segment_size '8388608' 'Max segment size'
DEFINE_integer server_num 3 'Number of servers'
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_integer port 8100 "Port of the first server"
DEFINE_integer partition_num 3 'Number of key range partitions'

my_ip=$(hostname -I)
declare -a group_default_ports=('8100' '8101' '8102')
declare -a participants=('10.10.0.111' '10.10.0.112' '10.10.0.118')
group_prefix="replica_"
rocksdb_path_prefix="rocksdb_file_"

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

# The alias for printing to stderr
alias error=">&2 echo counter: "

# hostname prefers ipv6
IP=`hostname -i | awk '{print $NF}'`

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

if [ "$FLAGS_clean" == "0" ]; then
    rm -rf runtime
fi

export TCMALLOC_SAMPLE_PARAMETER=524288

for ((i=0; i<$FLAGS_partition_num; ++i)); do

    # Replica within the same group starts on the same port
    common_group_port=${group_default_ports[$i]}
    raft_peers=""

    for ((j=0; j<$FLAGS_server_num; ++j)); do
        participant_ip=$participants[$j]
        raft_peers="${raft_peers}${participant_ip}:$((${group_port})):0,"
    done

    mkdir -p runtime/"partition_${i}"
    cp ./counter_server runtime/"partition_${i}"
    cd runtime/"partition_${i}"
    ${VALGRIND} ./counter_server \
        -bthread_concurrency=${FLAGS_bthread_concurrency}\
        -crash_on_fatal_log=${FLAGS_crash_on_fatal} \
        -raft_max_segment_size=${FLAGS_max_segment_size} \
        -raft_sync=${FLAGS_sync} \
        -port="${common_group_port}" \
        -group= "${group_prefix}${i}"\
        -rocksdb_path="${rocksdb_path_prefix}${i}"
        -conf="${raft_peers}" > std.log 2>&1 &
    cd ../..

done