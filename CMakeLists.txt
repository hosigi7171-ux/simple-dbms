# mkdir build
# cd build
# cmake ..
# make
# ctest 

cmake_minimum_required(VERSION 3.10)
project(SimpleDBMS CXX)

# gdb debug mode
set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON) 

# gdb용 컴파일러 옵션
add_compile_options(-g -O0)

# CTest 모듈을 먼저 로드하여 테스트 기능을 활성화
include(CTest) 

# ------------------------------------------
# Google Test (GTest) Settings: FetchContent
# ------------------------------------------

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/release-1.11.0.zip
)

# GTest 소스를 가져와 빌드에 포함
FetchContent_MakeAvailable(googletest)

# --------------------------------------------------------------
# Define source files and set paths (reflecting the bpt/ folder)
# --------------------------------------------------------------

# Core Library Source Files
file(GLOB CORE_SOURCES
    bpt/src/*.cpp
    bpt/src/bptree/*.cpp
)
# main.cpp는 실행 파일에 직접 링크
list(REMOVE_ITEM CORE_SOURCES bpt/src/main.cpp) 

# Test Source Files
set(BUF_MGR_TEST_SOURCES
    bpt/test/buf_mgr_test.cpp
    bpt/test/FileMock.cpp
)

set(SIMPLE_INSERT_TEST_SOURCES
    bpt/test/simple_insert_test.cpp
    bpt/test/FileMock.cpp
)

set(HARD_INSERT_TEST_SOURCES
    bpt/test/hard_insert_test.cpp
    bpt/test/FileMock.cpp
)

set(DELETE_TEST_SOURCES
    bpt/test/delete_test.cpp
    bpt/test/FileMock.cpp
)

set(FIND_TEST_SOURCES
    bpt/test/find_test.cpp
    bpt/test/FileMock.cpp
)

# ----------------------------------------
# Building the main library and executable
# ----------------------------------------

# 코어 정적 라이브러리 생성
add_library(bpt STATIC ${CORE_SOURCES})

target_include_directories(bpt PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/include
)

# 메인 실행 파일 생성 및 링크
add_executable(main bpt/src/main.cpp)
target_include_directories(main PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/include
)
target_link_libraries(main PRIVATE bpt)

# ------------------------------------------
# Libraries for testing (excluding file.cpp)
# ------------------------------------------

# 테스트용 소스: file.cpp를 제외한 모든 소스
set(TEST_CORE_SOURCES ${CORE_SOURCES})
list(REMOVE_ITEM TEST_CORE_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bpt/src/file.cpp)

# 일반 테스트용 라이브러리 (RECORD_CNT=31)
add_library(bpt_test STATIC ${TEST_CORE_SOURCES})
target_include_directories(bpt_test PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/test
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/include
)
target_compile_definitions(bpt_test PUBLIC TEST_ENV)

# DELETE 테스트 전용 라이브러리 (RECORD_CNT=2)
add_library(bpt_test_small STATIC ${TEST_CORE_SOURCES})
target_include_directories(bpt_test_small PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/test
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/include
)
target_compile_definitions(bpt_test_small PUBLIC 
    TEST_ENV
    RECORD_CNT=2
    ENTRY_CNT=16
    LEAF_ORDER=3
    INTERNAL_ORDER=17
    NON_HEADER_PAGE_RESERVED=3816
)

# 테스트 라이브러리의 헤더 경로 설정 (Mock 헤더 우선)
target_include_directories(bpt_test PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/test # Mock 헤더 우선 검색
    ${CMAKE_CURRENT_SOURCE_DIR}/bpt/include
)

# 테스트 환경임을 명시
target_compile_definitions(bpt_test PUBLIC TEST_ENV)


# -----------------------------------------
# Build test executables and register CTest
# -----------------------------------------

# 버퍼 관리자 테스트 실행 파일
add_executable(buf_mgr_test ${BUF_MGR_TEST_SOURCES})
target_link_libraries(buf_mgr_test PRIVATE gtest_main bpt_test)
add_test(NAME BufMgrTest COMMAND buf_mgr_test)

# B+ Tree Insertion 테스트 실행 파일
add_executable(simple_insert_test ${SIMPLE_INSERT_TEST_SOURCES})
target_link_libraries(simple_insert_test PRIVATE gtest_main bpt_test)
add_test(NAME SimpleInsertTest COMMAND simple_insert_test)
add_executable(hard_insert_test ${HARD_INSERT_TEST_SOURCES})
target_link_libraries(hard_insert_test PRIVATE gtest_main bpt_test)
add_test(NAME HardInsertTest COMMAND hard_insert_test)

# B+ Tree Deletion 테스트 (작은 크기 사용)
add_executable(delete_test ${DELETE_TEST_SOURCES})
target_link_libraries(delete_test PRIVATE gtest_main bpt_test_small) # 작은 크기 라이브러리 사용
add_test(NAME HardDeleteTest COMMAND delete_test)

# B+ Tree Find 테스트
add_executable(find_test ${FIND_TEST_SOURCES})
target_link_libraries(find_test PRIVATE gtest_main bpt_test)
add_test(NAME FindTest COMMAND find_test)
