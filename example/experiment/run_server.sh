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
DEFINE_integer server_num 2 'Number of servers'
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_integer port 8100 "Port of the first server"
DEFINE_integer partition_num 1 'Number of key range partitions'

my_ip=$(hostname -I)
my_ip=`echo $my_ip | sed 's/ *$//g'`
declare -a group_default_ports=('8100' '8101' '8102')

for ((i=0; i<$FLAGS_partition_num; ++i)); do
    sudo ufw allow ${group_default_ports[$i]}/tcp
done

declare -a participants=('10.10.0.111' '10.10.0.112' '10.10.0.118')

# Runing this script will spin up one column as the table below:
#               Machine 1           Machine 2           Machine 3
# Group 1 ['10.10.0.111:8100', '10.10.0.112:8100', '10.10.0.118:8100']
# Group 2 ['10.10.0.111:8101', '10.10.0.112:8101', '10.10.0.118:8101']
# Group 3 ['10.10.0.111:8102', '10.10.0.112:8102', '10.10.0.118:8102']

group_prefix="replica_"
rocksdb_path_prefix="rocksdb_file_"

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

# The alias for printing to stderr
alias error=">&2 echo counter: "

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

if [ "$FLAGS_clean" == "0" ]; then
    rm -rf runtime
fi

export TCMALLOC_SAMPLE_PARAMETER=524288

# E.g if the current machine ip is 10.10.0.111
# This script starts:
#               Machine 1      
# Group 1 ['10.10.0.111:8100']
# Group 2 ['10.10.0.111:8101']
# Group 3 ['10.10.0.111:8102']

for ((i=0; i<$FLAGS_partition_num; ++i)); do

    group_port=${group_default_ports[$i]}
    group_name=${group_prefix}${i}
    group_rocksdb_path=${rocksdb_path_prefix}${i}
    group_participants=""

    for ((j=0; j<$FLAGS_server_num; ++j)); do
        if [ "$my_ip" == "${participants[$j]}" ]
        then
            participant_ip=${participants[$j]}
        else
            participant_ip=${participants[$j]}
        fi
        group_participants="${group_participants}${participant_ip}:$((${group_port})):0,"
    done

    echo "$group_participants"
    echo "group is"
    echo "${group_name}"

    mkdir -p runtime/"partition_${i}"
    cp ./counter_server runtime/"partition_${i}"
    cd runtime/"partition_${i}"

    ${VALGRIND} ./counter_server \
        -bthread_concurrency=${FLAGS_bthread_concurrency}\
        -crash_on_fatal_log=${FLAGS_crash_on_fatal} \
        -raft_max_segment_size=${FLAGS_max_segment_size} \
        -raft_sync=${FLAGS_sync} \
        -ip="${my_ip}" \
        -port="${group_port}" \
        -group="${group_name}" \
        -rocksdb_path="${group_rocksdb_path}" \
        -conf="${group_participants}" > std.log 2>&1 &
    cd ../..

done