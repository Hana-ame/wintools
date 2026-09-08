cmake_minimum_required(VERSION 3.18)
project(twitterpic)

# JNI wrapper source
add_library(jni-wrapper SHARED jni_wrapper.c)

# Include jniLibs directory (where libechproxy.so is located)
set(JNILIBS_DIR "${CMAKE_SOURCE_DIR}/../jniLibs/${ANDROID_ABI}")
include_directories(${JNILIBS_DIR})

# Link with Go library
target_link_libraries(jni-wrapper
    android
    log
    ${JNILIBS_DIR}/libechproxy.so
)
