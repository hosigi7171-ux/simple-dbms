# --- 환경 변수 설정 ---
TARGET = main
SRCDIR = src
LIBS = lib
CXX = g++ 
CFLAGS = -g -fPIC -I include
LDFLAGS = 

# --- 파일 목록 정의 ---
# 1. 모든 소스 파일 목록
SRCS_BPTREE = $(wildcard $(SRCDIR)/bptree/*.cpp)
SRCS_TXNMGR = $(wildcard $(SRCDIR)/txn_mgr/*.cpp)
SRCS_LOGMGR = $(wildcard $(SRCDIR)/log_mgr/*.cpp)
SRCS_ROOT = $(wildcard $(SRCDIR)/*.cpp)

# main.cpp는 라이브러리에 포함되지 않도록 분리
TARGET_SRC = $(SRCDIR)/main.cpp

# 라이브러리 소스 목록 (main.cpp 제외)
SRCS_FOR_LIB = $(filter-out $(TARGET_SRC), $(SRCS_ROOT)) $(SRCS_BPTREE) $(SRCS_TXNMGR) $(SRCS_LOGMGR)

# 오브젝트 파일 목록 (경로를 포함하여 중복 방지: src/main.o, src/bptree/bptree.o)
OBJS_FOR_LIB = $(SRCS_FOR_LIB:.cpp=.o)
TARGET_OBJ = $(TARGET_SRC:.cpp=.o)
OBJS = $(TARGET_OBJ) $(OBJS_FOR_LIB)


# --- 기본 규칙 ---
.PHONY: all clean static_library

# 'all' 타겟이 실행될 때마다 'clean'을 먼저 실행하여 빌드를 초기화합니다.
# 이렇게 하면 항상 깨끗한 상태에서 다시 빌드됩니다.
all: clean static_library $(TARGET)

## --- 컴파일 패턴 규칙 (C++ .cpp -> .o) ---

# 1. src/%.cpp -> src/%.o (main, db_api, file, buf_mgr)
# 예: src/db_api.o를 만들기 위해 src/db_api.cpp 사용
$(SRCDIR)/%.o: $(SRCDIR)/%.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

# 2. src/bptree/%.cpp -> src/bptree/%.o
# 예: src/bptree/bptree.o를 만들기 위해 src/bptree/bptree.cpp 사용
$(SRCDIR)/bptree/%.o: $(SRCDIR)/bptree/%.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

$(SRCDIR)/txn_mgr/%.o: $(SRCDIR)/txn_mgr/%.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

$(SRCDIR)/log_mgr/%.o: $(SRCDIR)/log_mgr/%.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

## --- 라이브러리 및 실행 파일 생성 ---

# Create static library (libbpt.a)
static_library: $(OBJS_FOR_LIB)
	@mkdir -p $(LIBS)
	ar cr $(LIBS)/libbpt.a $(OBJS_FOR_LIB)

# Link main with static library
$(TARGET): $(TARGET_OBJ) $(LIBS)/libbpt.a
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(TARGET_OBJ) -L$(LIBS) -lbpt

# --- 정리 규칙 ---
# 모든 빌드 결과물을 삭제합니다.
clean:
	@echo "Cleaning previous build artifacts..."
	-rm -f $(TARGET) $(OBJS) $(LIBS)/libbpt.a