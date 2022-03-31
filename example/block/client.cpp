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
#include "block.pb.h"

DEFINE_bool(log_each_request, false, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(block_size, 64 * 1024 * 1024u, "Size of block");
DEFINE_int32(request_size, 16, "Size of each requst");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(write_percentage, 100, "Percentage of fetch_add");
DEFINE_string(conf, "", "Configuration of the raft group");
DEFINE_string(group, "Block", "Id of the replication group");

bvar::LatencyRecorder g_latency_recorder("block_client");

static void* sender(void* arg) {
    size_t offset = 0;
    while (!brpc::IsAskedToQuit()) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                        FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        example::BlockService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        // Randomly select which request we want send;
        example::BlockRequest request;
        example::BlockResponse response;
        request.set_offset(offset);
        bool isread = false;
        const char* op = NULL;
        if (butil::fast_rand_less_than(100) < (size_t)FLAGS_write_percentage) {
            op = "write";
            cntl.request_attachment().resize(FLAGS_request_size, 'a');
            //std::cout << cntl.request_attachment() << std::endl;
            stub.write(&cntl, &request, &response, NULL);
            offset = offset + FLAGS_request_size;
        } else {
            op = "read";
            isread = true;
            if(offset == 0) {
                continue;
            }
            request.set_offset(offset - FLAGS_request_size);
            request.set_size(FLAGS_request_size);
            stub.read(&cntl, &request, &response, NULL);
        }
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue;
        }
        if (!response.success()) {
            LOG(WARNING) << "Fail to send request to " << leader
                         << ", redirecting to "
                         << (response.has_redirect() 
                                ? response.redirect() : "nowhere");
            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response.redirect());
            continue;
        }
        g_latency_recorder << cntl.latency_us();
        if (FLAGS_log_each_request) {
            if(isread){
                //https://github.com/apache/incubator-brpc/blob/master/src/butil/iobuf.h
                std::string response_attachment_str = cntl.response_attachment().to_string();
                printf("request length is %lu\n", cntl.response_attachment().size());
                // char * strobj = (char*) malloc (cntl.response_attachment().size() + 1);
                // cntl.response_attachment().copy_to(strobj, cntl.response_attachment().size());
                // strobj[cntl.response_attachment().size() + 1] = '\0';
                std::cout << "String is " << response_attachment_str << "\n";
            }
            LOG(INFO) << "Received response from " << leader
                      << " op=" << op
                      << " offset=" << request.offset()
                      << " request_attachment="
                      << cntl.request_attachment().size()
                      << " response_attachment="
                      << cntl.response_attachment().size()
                      << " latency=" << cntl.latency_us();
            bthread_usleep(1000L * 1000L);
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    std::vector<bthread_t> tids;
    tids.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread) {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&tids[i], NULL, sender, NULL) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
        LOG_IF(INFO, !FLAGS_log_each_request)
                << "Sending Request to " << FLAGS_group
                << " (" << FLAGS_conf << ')'
                << " at qps=" << g_latency_recorder.qps(1)
                << " latency=" << g_latency_recorder.latency(1);
    }

    LOG(INFO) << "Block client is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(tids[i], NULL);
        } else {
            bthread_join(tids[i], NULL);
        }
    }

    return 0;
}
