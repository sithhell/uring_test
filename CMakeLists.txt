# 
# Created by th on 3/29/24.
# Copyright (c) 2024 Exasol AG. All rights reserved.
#

cmake_minimum_required(VERSION 3.20)
project(uring_test CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_CXX_STANDARD 20)

find_package(PkgConfig)
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET GLOBAL liburing>=2.0)

add_executable(msg_ring msg_ring.cpp)
target_link_libraries(msg_ring PUBLIC PkgConfig::liburing)
target_include_directories(msg_ring PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

add_executable(ping_pong ping_pong.cpp)
target_link_libraries(ping_pong PUBLIC PkgConfig::liburing)
target_include_directories(ping_pong PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
