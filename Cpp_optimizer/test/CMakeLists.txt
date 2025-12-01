cmake_minimum_required(VERSION 3.10)
project(OptimizerCpp)

set(CMAKE_CXX_STANDARD 17)

add_executable(optimizer 
    main.cpp 
    config.h 
    utils.h 
    data_loader.h 
    initialization.h 
    objectives.h 
    algorithm.h
)
