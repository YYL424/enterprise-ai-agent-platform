cmake_minimum_required(VERSION 3.15)
project(compute_engine_core_project LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. 精准锁定当前激活的 Conda 环境中的 Python 解释器和开发头文件
find_package(Python REQUIRED COMPONENTS Interpreter Development)

# 2. 动态问出 pybind11 在 site-packages 中的物理 CMake 路径
execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE PYBIND11_INTERN_CMAKE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# 3. 将其注入到 CMake 的高优先级搜寻链中并加载
message(STATUS "Auto-detected pybind11 CMake path: ${PYBIND11_INTERN_CMAKE_DIR}")
find_package(pybind11 REQUIRED PATHS ${PYBIND11_INTERN_CMAKE_DIR} NO_DEFAULT_PATH)

# 🌟【本次修复核心】显式向系统申领并加载 Linux 原生 pthread 多线程组件
find_package(Threads REQUIRED)

# 4. 声明创建 Python 核心二进制扩展模块
pybind11_add_module(compute_engine_core core_bind.cpp)

# 5. 引入当前头文件路径
target_include_directories(compute_engine_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# 6. 开启 O3 极致性能优化并隐藏符号表
target_compile_options(compute_engine_core PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang>:-O3 -fvisibility=hidden -pthread>
)

# 7. 链接 Linux 原生多线程库（此时 Threads::Threads 目标已合法存在）
target_link_libraries(compute_engine_core PRIVATE Threads::Threads)