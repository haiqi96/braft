// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include "keyvalue.pb.h"
#include <boost/functional/hash.hpp>
#include "OPcode.h"

DEFINE_bool(log_each_request, false, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(num_groups, 1, "Number of replication groups");
DEFINE_int32(update_percentage, 50, "Percentage of update_database");
DEFINE_int64(added_by, 1, "Num added to each peer");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 1000, "Timeout for each request");
DEFINE_string(conf, "", "Configuration of the raft group");
DEFINE_string(group, "Counter", "Id of the replication group");
DEFINE_int32(op, 0, "Op code for what operation to perform");
DEFINE_string(key, "20", "Key to read/written/deleted");
DEFINE_string(value, "abc", "The value corresponding to the key argument");

bvar::LatencyRecorder g_latency_recorder("counter_client");

struct client_args{
    int op;
    std::string key;
    std::string value = "";
};

// int stringHash(std::string const& s) {
//     unsigned long hash = 5381;
//     for (auto c : s) {
//         hash = (hash << 5) + hash + c; /* hash * 33 + c */
//     }
//     return hash;
// }

static void *sender(void *arg)
{
    int read_index = 0;
    int write_index = 0;
    struct client_args * cl_args = (struct client_args *)arg;
    std::cout << cl_args->op <<" : "<<cl_args->key<<" : "<<cl_args->value<<std::endl; 
    
    // Send to the leader group
    boost::hash<std::string> stringHash;
    int hash_code  = stringHash(cl_args->key);
    std::string replica_group = "replica_" + std::to_string(hash_code % FLAGS_num_groups);
    while (!brpc::IsAskedToQuit())
    {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(replica_group, &leader) != 0)
        {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok())
            {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0)
        {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        keyvalue::KeyValueService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        // Randomly select which request we want send;

        if (OP_WRITE == cl_args->op || OP_DELETE == cl_args->op || OP_MODIFY == cl_args->op)
        {
            keyvalue::InsertRequest request;
            keyvalue::InsertResponse response;

            std::string test_key = cl_args->key;
            std::string test_value = cl_args->value;
            request.set_key(test_key);
            request.set_op(cl_args->op);
            request.set_value(test_value);
            if (OP_DELETE == cl_args->op){
                std::cout << "delete " << test_key << std::endl;
            }
            else {
                std::cout << "write " << test_key << " : " <<  test_value << std::endl;
            }
            stub.insert(&cntl, &request, &response, NULL);

            if (cntl.Failed())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << " : " << cntl.ErrorText();
                // Clear leadership since this RPC failed.
                braft::rtb::update_leader(FLAGS_group, braft::PeerId());
                bthread_usleep(FLAGS_timeout_ms * 1000L);
                continue;
            }

            if (!response.success())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << ", redirecting to "
                             << (response.has_redirect()
                                     ? response.redirect()
                                     : "nowhere");
                // Update route table since we have redirect information
                braft::rtb::update_leader(FLAGS_group, response.redirect());
                continue;
            }

            g_latency_recorder << cntl.latency_us();
            if (FLAGS_log_each_request)
            {
                if (OP_DELETE == cl_args->op){
                    LOG(INFO) << "Received write response from " << leader
                          << ", deleted successfully";
                }
                else {
                    LOG(INFO) << "Received write response from " << leader
                          << ", insert successfully";
                }
                bthread_usleep(1000L * 1000L);
            }
        } 
        else if(OP_READ == cl_args->op)
        {
            keyvalue::GetRequest request;
            keyvalue::GetResponse response;

            printf("read\n");
            std::string read_test_key = cl_args->key;
            request.set_key(read_test_key);
            stub.get(&cntl, &request, &response, NULL);

            if (cntl.Failed())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << " : " << cntl.ErrorText();
                // Clear leadership since this RPC failed.
                braft::rtb::update_leader(FLAGS_group, braft::PeerId());
                bthread_usleep(FLAGS_timeout_ms * 1000L);
                continue;
            }

            if (!response.success())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << ", redirecting to "
                             << (response.has_redirect()
                                     ? response.redirect()
                                     : "nowhere");
                // Update route table since we have redirect information
                braft::rtb::update_leader(FLAGS_group, response.redirect());
                continue;
            }
            std::cout << "read " << read_test_key << " : " <<  response.value() << std::endl;
            if(read_index < write_index - 1){
                read_index++;
            }
            g_latency_recorder << cntl.latency_us();
            if (FLAGS_log_each_request)
            {
                LOG(INFO) << "Received read response from " << leader
                          << " value=" << response.value()
                          << " latency=" << cntl.latency_us();
                bthread_usleep(1000L * 1000L);
            }
        }
        break;
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    struct client_args * cl_args = (struct client_args *) malloc(sizeof(struct client_args));
    // cl_args->key = FLAGS_key;
    // cl_args->value = FLAGS_value;
    // cl_args->op = FLAGS_op;
    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0)
    {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }
    std::vector<bthread_t> tids;
    while (!brpc::IsAskedToQuit()){
        int cin_op;
        std::string cin_key;
        std::string cin_value;
        std::cout << "Enter op code (0, 1, 2 or 3) press -1 to exit: ";
        std::cin >> cin_op;
        if(cin_op == -1){
            free(cl_args);  
            return 0;
        }
        cl_args->op = cin_op;
        std::cout << "Enter the key :";
        std::cin >> cin_key;
        cl_args->key = cin_key;
        if((OP_READ != cin_op) &&  (OP_DELETE != cin_op)){
            std::cout << "Enter the value :";
            std::cin >> cin_value;
            cl_args->value = cin_value;
        }
        tids.resize(FLAGS_thread_num);
        if (!FLAGS_use_bthread)
        {
            for (int i = 0; i < FLAGS_thread_num; ++i)
            {
                if (pthread_create(&tids[i], NULL, sender, cl_args) != 0)
                {
                    LOG(ERROR) << "Fail to create pthread";
                    return -1;
                }
            }
        }
        else
        {
            for (int i = 0; i < FLAGS_thread_num; ++i)
            {
                if (bthread_start_background(&tids[i], NULL, sender, cl_args) != 0)
                {
                    LOG(ERROR) << "Fail to create bthread";
                    return -1;
                }
            }
        }

        // Wait until the threads end
        for (int i = 0; i < FLAGS_thread_num; ++i)
            {
                if (!FLAGS_use_bthread)
                {
                    pthread_join(tids[i], NULL);
                }
                else
                {
                    bthread_join(tids[i], NULL);
                }
            }
    }

    // while (!brpc::IsAskedToQuit())
    // {
    //     sleep(1);
    //     LOG_IF(INFO, !FLAGS_log_each_request)
    //         << "Sending Request to " << FLAGS_group
    //         << " (" << FLAGS_conf << ')'
    //         << " at qps=" << g_latency_recorder.qps(1)
    //         << " latency=" << g_latency_recorder.latency(1);
    // }

    LOG(INFO) << "Counter client is going to quit";

    free(cl_args);
    return 0;
}
