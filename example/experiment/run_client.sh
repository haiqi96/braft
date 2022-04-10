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
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'
DEFINE_integer update_percentage 50 'Percentage of fetch_add operation'
DEFINE_integer bthread_concurrency '8' 'Number of worker pthreads'
DEFINE_integer server_port 8100 "Port of the first server"
DEFINE_integer server_num 3 'Number of servers'
DEFINE_integer thread_num 1 'Number of sending thread'
DEFINE_string crash_on_fatal 'true' 'Crash on fatal log'
DEFINE_string log_each_request 'true' 'Print log for each request'
DEFINE_string valgrind 'false' 'Run in valgrind'
DEFINE_string use_bthread "true" "Use bthread to send request"

FLAGS "$@" || exit 1

# hostname prefers ipv6
IP=`hostname -i | awk '{print $NF}'`

if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
fi

declare -a participants=('10.10.0.111' '10.10.0.112' '10.10.0.118')
group_port='8100'

group_participants=""
for ((j=0; j<$FLAGS_server_num; ++j)); do
    participant_ip=${participants[$j]}
    group_participants="${group_participants}${participant_ip}:$((${group_port})):0,"
done

export TCMALLOC_SAMPLE_PARAMETER=524288

group_prefix="replica_"
i=0
${VALGRIND} ./counter_client \
        --update_percentage=${FLAGS_update_percentage} \
        --bthread_concurrency=${FLAGS_bthread_concurrency} \
        --conf="${group_participants}" \
        --crash_on_fatal_log=${FLAGS_crash_on_fatal} \
        --log_each_request=${FLAGS_log_each_request} \
        --thread_num=${FLAGS_thread_num} \
        --group= "${group_prefix}${i}"\
        --use_bthread=${FLAGS_use_bthread} \

