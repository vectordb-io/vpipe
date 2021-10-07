#include <iostream>
#include "vpipe.h"
#include "util.h"

int
main(int argc, char **argv) {
    vectordb::VPipe vpipe("./test_vpipe");
    vpipe.Create();

    int count = 20;
    for (int i = 0; i < count; ++i) {
        char buf[128];
        snprintf(buf, sizeof(buf), "message_%d", i);
        auto b = vpipe.Produce(std::string(buf));
        assert(b);
        std::cout << "produce " << std::string(buf) << std::endl;
    }
    std::cout << std::endl;

    int count2 = 5;
    for (int i = 0; i < count2; ++i) {
        std::string msg;
        auto b = vpipe.Consume("consumer1", msg);
        assert(b);
        std::cout << "consumer1 consume" << msg << std::endl;
    }
    std::cout << std::endl;

    for (int i = 0; i < count2; ++i) {
        std::string msg;
        auto b = vpipe.Consume("consumer1", msg);
        assert(b);
        std::cout << "consumer1 consume" << msg << std::endl;
    }
    std::cout << std::endl;

    for (int i = 0; i < count2; ++i) {
        std::string msg;
        auto b = vpipe.Consume("consumer2", msg);
        assert(b);
        std::cout << "consumer2 consume" << msg << std::endl;
    }
    std::cout << std::endl;

    return 0;
}
