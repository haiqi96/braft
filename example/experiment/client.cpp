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
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include "keyvalue.pb.h"
#include <boost/functional/hash.hpp>
#include "OPcode.h"
#include "echo.pb.h"

// This is for the RPC server
DEFINE_bool(echo_attachment, false, "Echo attachment as well");
DEFINE_int32(port, 8500, "TCP Port of this server");
DEFINE_string(listen_addr, "", "Server listen address, may be IPV4/IPV6/UDS."
            " If this is set, the flag port will be ignored");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

DEFINE_bool(log_each_request, false, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(num_groups, 1, "Number of replication groups");
DEFINE_int32(update_percentage, 50, "Percentage of update_database");
DEFINE_int64(added_by, 1, "Num added to each peer");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 1000, "Timeout for each request");
DEFINE_string(conf, "", "Configuration of the raft group");
DEFINE_string(group, "Counter", "Id of the replication group");

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


static void *dummy_sender(void *arg)
{
    struct client_args * cl_args = (struct client_args *)arg;
    // Send to the leader group
    boost::hash<std::string> stringHash;
    std::string test_key = "";
    int hash_code  = stringHash(test_key);
    std::string replica_group = "replica_" + std::to_string(hash_code % FLAGS_num_groups);
    while (!brpc::IsAskedToQuit())
    {
        unsigned op_code = cl_args->op;
        // For any non-read operation call the stub.insert rpc call
        if (OP_WRITE == op_code || OP_DELETE == op_code || OP_MODIFY == op_code)
        {
            keyvalue::InsertRequest request;
            keyvalue::InsertResponse response;

            std::string test_value = "";
            request.set_key(test_key);
            request.set_op(op_code);
            request.set_value(test_value);
            if (OP_DELETE == op_code){
                std::cout << "delete " << test_key << std::endl;
            }
            else {
                std::cout << "write " << test_key << " : " << std::endl;
            }
        } 
        else if(OP_READ == op_code)
        {
            keyvalue::GetRequest request;
            keyvalue::GetResponse response;

            std::string read_test_key = test_key;
            printf("read key %s\n");
        }
        // If either operation is successful, break out of the loop and accept another op
        break;
    }
    return NULL;
}

static void *sender(void *arg)
{

    int read_index = 0;
    int write_index = 0;
    // Send to the leader group
    std::string replica_group = "replica_0"; 
    while (!brpc::IsAskedToQuit())
    {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(replica_group, &leader) != 0)
        {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                replica_group, FLAGS_timeout_ms);
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
        if (butil::fast_rand_less_than(100) < (size_t)FLAGS_update_percentage)
        {
            keyvalue::InsertRequest request;
            keyvalue::InsertResponse response;

            std::string test_key = "test_key" + std::to_string(write_index);
            std::string test_value = "test_value" + std::to_string(write_index);
            request.set_key(test_key);
            request.set_value(test_value);
            request.set_op(OP_WRITE);
            if (OP_DELETE == request.op()){
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
                braft::rtb::update_leader(replica_group, braft::PeerId());
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
                braft::rtb::update_leader(replica_group, response.redirect());
                continue;
            }

            g_latency_recorder << cntl.latency_us();
            if (FLAGS_log_each_request)
            {
                if (OP_DELETE == request.op()){
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
        else
        {
            keyvalue::GetRequest request;
            keyvalue::GetResponse response;

            printf("read\n");
            std::string read_test_key = "test_key" + std::to_string(read_index);
            request.set_key(read_test_key);
            stub.get(&cntl, &request, &response, NULL);

            if (cntl.Failed())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << " : " << cntl.ErrorText();
                // Clear leadership since this RPC failed.
                braft::rtb::update_leader(replica_group, braft::PeerId());
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
                braft::rtb::update_leader(replica_group, response.redirect());
                continue;
            }
            std::cout << "read " << read_test_key << " : " <<  response.value() << std::endl;

            g_latency_recorder << cntl.latency_us();
            if (FLAGS_log_each_request)
            {
                LOG(INFO) << "Received read response from " << leader
                          << " value=" << response.value()
                          << " latency=" << cntl.latency_us();
                bthread_usleep(1000L * 1000L);
            }
        }
    }
    return NULL;
}

// Your implementation of example::EchoService
// Notice that implementing brpc::Describable grants the ability to put
// additional information in /status.
namespace example {
class EchoServiceImpl : public EchoService {
public:
    EchoServiceImpl() {};
    virtual ~EchoServiceImpl() {};
    virtual void Echo(google::protobuf::RpcController* cntl_base,
                      const EchoRequest* request,
                      EchoResponse* response,
                      google::protobuf::Closure* done) {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        struct client_args cl_args;
        cl_args.op = request->op();
        cl_args.key = request->key();
        cl_args.value = request->value();
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);
        std::vector<bthread_t> tids;
        // The purpose of following logs is to help you to understand
        // how clients interact with servers more intuitively. You should 
        // remove these logs in performance-sensitive servers.
        tids.resize(FLAGS_thread_num);
        if (!FLAGS_use_bthread)
        {
            for (int i = 0; i < FLAGS_thread_num; ++i)
            {
                if (pthread_create(&tids[i], NULL, dummy_sender, &cl_args) != 0)
                {
                    LOG(ERROR) << "Fail to create pthread";
                    response->set_status(STATUS_KERROR);
                }
            }
        }
        else
        {
            for (int i = 0; i < FLAGS_thread_num; ++i)
            {
                if (bthread_start_background(&tids[i], NULL, dummy_sender, &cl_args) != 0)
                {
                    LOG(ERROR) << "Fail to create bthread";
                    response->set_status(STATUS_KERROR);
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
        response->set_status(STATUS_KOK);
    }
};
}  // namespace example



int main(int argc, char *argv[])
{
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0)
    {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    std::vector<bthread_t> tids;
    tids.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread)
    {
        for (int i = 0; i < FLAGS_thread_num; ++i)
        {
            if (pthread_create(&tids[i], NULL, sender, NULL) != 0)
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
            if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0)
            {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit())
    {
        sleep(1);
        LOG_IF(INFO, !FLAGS_log_each_request)
            << "Sending Request to " << FLAGS_group
            << " (" << FLAGS_conf << ')'
            << " at qps=" << g_latency_recorder.qps(1)
            << " latency=" << g_latency_recorder.latency(1);
    }

    LOG(INFO) << "Counter client is going to quit";
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

    return 0;
}

