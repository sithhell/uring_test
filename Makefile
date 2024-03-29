
CXX = g++
CXXFLAGS = -std=c++20 -I. $(shell pkg-config --cflags liburing)
LDFLAGS = $(shell pkg-config --libs liburing)

SOURCES = msg_ring.cpp ping_pong.cpp
EXECUTABLES = $(SOURCES:.cpp=)


%: %.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $< -o $@

.PHONY = all
all: $(EXECUTABLES)

.PHONY = clean
clean:
	rm -rf $(EXECUTABLES)
