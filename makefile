OS := $(shell uname)
ifeq ($(OS),Darwin)
	CC	= /opt/homebrew/bin/g++-12
	CFLAGS  = -O3 -std=c++14 -w -march=native -I/opt/homebrew/Cellar/boost/1.79.0_2/include
   	LDFLAGS =
else
	CC      = g++
	CFLAGS  = -O3 -mavx -std=c++14 -w
	LDFLAGS =
endif

SOURCES = utils.cpp containers/relation.cpp containers/offsets_templates.cpp containers/offsets.cpp indices/1dgrid.cpp indices/hierarchicalindex.cpp indices/hint.cpp indices/hint_m.cpp indices/hint_m_subs+sort.cpp indices/hint_m_subs+sopt.cpp indices/hint_m_subs+sort+sopt.cpp indices/hint_m_subs+sort+sopt+ss.cpp indices/hint_m_subs+sort+sopt+cm.cpp indices/hint_m_subs+sort+ss+cm.cpp indices/hint_m_all.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: query

query: lscan 1dgrid hint hint_m


lscan: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o main_lscan.cpp -o query_lscan.exec $(LDADD)

1dgrid: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/1dgrid.o main_1dgrid.cpp -o query_1dgrid.exec $(LDADD)

hint: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint.o main_hint.cpp -o query_hint.exec $(LDADD)

hint_m: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o containers/offsets_templates.o containers/offsets.o indices/hierarchicalindex.o indices/hint_m.o indices/hint_m_subs+sort.o indices/hint_m_subs+sopt.o indices/hint_m_subs+sort+sopt.o indices/hint_m_subs+sort+sopt+ss.o indices/hint_m_subs+sort+sopt+cm.cpp indices/hint_m_subs+sort+ss+cm.o indices/hint_m_all.o main_hint_m.cpp -o query_hint_m.exec $(LDADD)


.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf utils.o
	rm -rf containers/*.o
	rm -rf indices/*.o
	rm -rf query_lscan.exec
	rm -rf query_1dgrid.exec
	rm -rf query_hint.exec
	rm -rf query_hint_m.exec
