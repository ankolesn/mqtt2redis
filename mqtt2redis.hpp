//
// Created by ankolesn on 30.07.22.
//

#ifndef MQTT2REDIS_MQTT2REDIS_HPP
#define MQTT2REDIS_MQTT2REDIS_HPP

#include <string>
#include <vector>
#include <thread>
#include "concurrent-queue/queue.hpp"
#include <sw/redis++/redis++.h>
#include <sw/redis++/redis.h>
#include <atomic>
#include <mosquittopp.h>
#include <mosquitto.h>
#include <fstream>
#include <map>

inline Concurrent_queue<std::pair<std::string, std::string>> cq;

class mqtt2redis {
private:
    std::string mqtt_ip = "";
    int mqtt_port = 0;
    std::map<std::string, std::string> params;
    const uint16_t thread_num = std::thread::hardware_concurrency();
    sw::redis::ConnectionOptions opts1;
    std::unique_ptr<sw::redis::Redis> r;
    std::atomic<bool> is_stopped = false;

public:
    mqtt2redis();
    static void on_connect(struct mosquitto *mosq, void *obj, int reason_code);

    static void on_subscribe(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos);

    static void on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg);

    void write_to(const std::string &key, const std::string &value);

    void run();

    void read_from();

    void read_from_file();

    [[noreturn]] void do_work() {
        std::pair<std::string, std::string> p;
        while (true) {
            cq.wait_and_pop(p);
            write_to(p.first, p.second);
        }
    }
};


#endif //MQTT2REDIS_MQTT2REDIS_HPP
