CXX = g++
# C++ 표준과 경고 옵션 설정
CXXFLAGS = -g -O2 -Wall -std=c++11
# 라이브러리와 헤더 경로 (LDFLAGS는 링크 시 뒤에 붙도록 설정)
LDFLAGS = -L. -I.
LDLIBS = -lbpt -lpthread

# 생성할 테스트 실행 파일 목록
TARGETS = test_basic test_redo test_undo 

all: $(TARGETS)

# C++ 라이브러리(libbpt.a)와의 호환성을 위해 g++을 사용합니다.
test_basic: test_basic.c
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

test_redo: test_redo.c
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

test_undo: test_undo.c
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGETS) *.o DATA*.db logfile.data logmsg.txt
