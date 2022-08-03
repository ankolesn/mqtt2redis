cmake_minimum_required(VERSION 3.22)
project(mqtt2redis)

set(CMAKE_CXX_STANDARD 17)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(mqtt2redis main.cpp mqtt2redis.hpp mqtt2redis.cpp)

target_link_libraries(mqtt2redis PUBLIC ${CONAN_LIBS})
