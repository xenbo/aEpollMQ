cmake_minimum_required(VERSION 3.8)
project(aeLevelDB)

set(CMAKE_CXX_STANDARD 98)

include_directories(.)
link_directories(. aEpoll)
set(SOURCE_FILES testAeMain.cpp getcmdToAe.cpp getcmdToAe.h opleveldb.cpp opleveldb.h job.cpp job.h aePipe.cpp aePipe.h  aEpoll/ae.cpp )
add_executable(aeLevelDB ${SOURCE_FILES})

target_link_libraries(aeLevelDB leveldb pthread)