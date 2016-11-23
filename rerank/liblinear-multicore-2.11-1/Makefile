CXX ?= g++
CC ?= gcc
CFLAGS = -Wall -Wconversion -O3 -fPIC -fopenmp
LIBS ?= blas/blas.a
#LIBS ?= -lblas
SHVER = 3
OS = $(shell uname)

all: train predict

lib: linear.o tron.o blas/blas.a
	if [ "$(OS)" = "Darwin" ]; then \
        SHARED_LIB_FLAG="-dynamiclib -Wl,-install_name,liblinear.so.$(SHVER)"; \
    else \
        SHARED_LIB_FLAG="-shared -Wl,-soname,liblinear.so.$(SHVER)"; \
    fi; \
    $(CXX) -fopenmp $${SHARED_LIB_FLAG} linear.o tron.o blas/blas.a -o liblinear.so.$(SHVER)

train: tron.o linear.o train.c blas/blas.a eval.o
	$(CXX) $(CFLAGS) -o train train.c tron.o eval.o linear.o $(LIBS)

predict: tron.o linear.o predict.c blas/blas.a eval.o
	$(CXX) $(CFLAGS) -o predict predict.c tron.o eval.o linear.o $(LIBS)

eval.o: eval.h eval.cpp
	$(CXX) $(CFLAGS) -c -o eval.o eval.cpp

tron.o: tron.cpp tron.h
	$(CXX) $(CFLAGS) -c -o tron.o tron.cpp

linear.o: linear.cpp linear.h
	$(CXX) $(CFLAGS) -c -o linear.o linear.cpp

blas/blas.a: blas/*.c blas/*.h
	make -C blas OPTFLAGS='$(CFLAGS)' CC='$(CC)';

clean:
	make -C blas clean
	make -C matlab clean
	rm -f *~ tron.o linear.o train predict liblinear.so.$(SHVER)
