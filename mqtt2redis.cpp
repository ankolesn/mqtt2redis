//
// Created by ankolesn on 30.07.22.
//

#include "mqtt2redis.hpp"

void mqtt2redis::on_connect(struct mosquitto *mosq, void *obj, int reason_code) {
    int rc;
    printf("on_connect: %s\n", mosquitto_connack_string(reason_code));
    if (reason_code != 0) {
        mosquitto_disconnect(mosq);
    }
    rc = mosquitto_subscribe(mosq, nullptr, "topic2/#", 1);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "Error subscribing: %s\n", mosquitto_strerror(rc));
        mosquitto_disconnect(mosq);
    }
}

void mqtt2redis::on_subscribe(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos) {
    int i;
    bool have_subscription = false;
    for (i = 0; i < qos_count; i++) {
        printf("on_subscribe: %d:granted qos = %d\n", i, granted_qos[i]);
        if (granted_qos[i] <= 2) {
            have_subscription = true;
        }
    }
    if (!have_subscription) {
        fprintf(stderr, "Error: All subscriptions rejected.\n");
        mosquitto_disconnect(mosq);
    }
}

void mqtt2redis::on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg) {
    cq.push(std::make_pair(msg->topic, (char *) msg->payload));
}

void mqtt2redis::write_to(const std::string &key, const std::string &value) {
    if (params.find(key) != params.end()) {
        r->publish(params[key], value);
    } else{
        r->publish(key, value);
    }
}

void mqtt2redis::run() {
    std::vector<std::thread> threads;
    threads.emplace_back(std::thread(&mqtt2redis::read_from, this));
    for (auto i = 1; i < thread_num; ++i) {
        threads.emplace_back(std::thread(&mqtt2redis::do_work, this));
    }
    threads[0].detach();
    for (auto &&t: threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void mqtt2redis::read_from() {
    struct mosquitto *mosq;
    int rc;
    mosquitto_lib_init();
    mosq = mosquitto_new(nullptr, true, nullptr);
    if (mosq == nullptr) {
        fprintf(stderr, "Error: Out of memory.\n");
        return;
    }
    mosquitto_connect_callback_set(mosq, &mqtt2redis::on_connect);
    mosquitto_subscribe_callback_set(mosq, &mqtt2redis::on_subscribe);
    mosquitto_message_callback_set(mosq, &on_message);

    rc = mosquitto_connect(mosq, mqtt_ip.c_str(), mqtt_port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        mosquitto_destroy(mosq);
        fprintf(stderr, "Error: %s\n", mosquitto_strerror(rc));
        return;
    }

    mosquitto_loop_forever(mosq, -1, 1);

    mosquitto_lib_cleanup();
}

mqtt2redis::mqtt2redis() {
    read_from_file();
    opts1.host = params["REDIS_IP"];
    opts1.port = stoi(params["REDIS_PORT"]);

    mqtt_ip = params["MQTT_IP"];
    mqtt_port = stoi(params["MQTT_PORT"]);;

    opts1.socket_timeout = std::chrono::milliseconds(100);
    r = std::make_unique<sw::redis::Redis>((opts1));
}

void mqtt2redis::read_from_file() {
    std::string s;
    std::ifstream file("../file");
    if (!file.is_open()) {
        throw std::invalid_argument("There is no file named file!");
    }
    while (!file.eof()) {
        std::getline(file, s);
        s.erase(std::remove(s.begin(), s.end(), ' '), s.end());
        if (s[0] == '#') {
            continue;
        }
        auto pos = s.find('=');
        params[s.substr(0, pos)] = s.substr(pos + 1);
    }
    // default parametrs
    params.try_emplace("REDIS_IP", "127.0.0.1");
    params.try_emplace("MQTT_IP", "127.0.0.1");
    params.try_emplace("REDIS_PORT", "6379");
    params.try_emplace("MQTT_PORT", "1883");
}