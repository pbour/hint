/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2020 - 2022
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#pragma once
#ifndef _GLOBAL_DEF_H_
#define _GLOBAL_DEF_H_

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <unistd.h>
#include <tuple>
using namespace std;

// Comment out the following for XOR workload
//#define WORKLOAD_COUNT

// Basic predicates of Allen's algebra
#define PREDICATE_EQUALS     1
#define PREDICATE_STARTS     2
#define PREDICATE_STARTED    3
#define PREDICATE_FINISHES   4
#define PREDICATE_FINISHED   5
#define PREDICATE_MEETS      6
#define PREDICATE_MET        7
#define PREDICATE_OVERLAPS   8
#define PREDICATE_OVERLAPPED 9
#define PREDICATE_CONTAINS   10
#define PREDICATE_CONTAINED  11
#define PREDICATE_PRECEDES   12
#define PREDICATE_PRECEDED   13

// Generalized predicates, ACM SIGMOD'22 gOverlaps
#define PREDICATE_GOVERLAPS  14


#define HINT_OPTIMIZATIONS_NO          0
#define HINT_OPTIMIZATIONS_SS          1

#define HINT_M_OPTIMIZATIONS_NO                   0
#define HINT_M_OPTIMIZATIONS_SUBS                 1
#define HINT_M_OPTIMIZATIONS_SUBS_SORT            2
#define HINT_M_OPTIMIZATIONS_SUBS_SOPT            3
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT       4
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS    5
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM    6
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SS_CM      7
#define HINT_M_OPTIMIZATIONS_ALL                  8


typedef int PartitionId;
typedef int RecordId;
typedef int Timestamp;


struct RunSettings
{
	string       method;
	const char   *dataFile;
	const char   *queryFile;
	bool         verbose;
    unsigned int typeQuery;
    unsigned int typePredicate;
	unsigned int numPartitions;
	unsigned int numBits;
	unsigned int maxBits;
	bool         topDown;
	bool         isAutoTuned;
	unsigned int numRuns;
    unsigned int typeOptimizations;
	
	void init()
	{
		verbose	          = false;
		topDown           = false;
		isAutoTuned       = false;
		numRuns           = 1;
        typeOptimizations = 0;
	};
};


struct StabbingQuery
{
	size_t id;
	Timestamp point;
    
    StabbingQuery()
    {
        
    };
    StabbingQuery(size_t i, Timestamp p)
    {
        id = i;
        point = p;
    };
};

struct RangeQuery
{
	size_t id;
	Timestamp start, end;

    RangeQuery()
    {
        
    };
    RangeQuery(size_t i, Timestamp s, Timestamp e)
    {
        id = i;
        start = s;
        end = e;
    };
};


class Timer
{
private:
	using Clock = std::chrono::high_resolution_clock;
	Clock::time_point start_time, stop_time;
	
public:
	Timer()
	{
		start();
	}
	
	void start()
	{
		start_time = Clock::now();
	}
	
	
	double getElapsedTimeInSeconds()
	{
		return std::chrono::duration<double>(stop_time - start_time).count();
	}
	
	
	double stop()
	{
		stop_time = Clock::now();
		return getElapsedTimeInSeconds();
	}
};


// Imports from utils
string toUpperCase(char *buf);
bool checkPredicate(string strPredicate, RunSettings &settings);
bool checkOptimizations(string strOptimizations, RunSettings &settings);
void process_mem_usage(double& vm_usage, double& resident_set);
#endif // _GLOBAL_DEF_H_
