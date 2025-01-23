/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   George Christodoulou
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

#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/hint.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT: A Hierarchical Index for Intervals in Main Memory" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_hint.exec [OPTION]... [DATA] [QUERIES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode; print the trace for every query; otherwise only the final report" << endl;
    cerr << "       -q predicate" << endl;
    cerr << "              set predicate type: \"EQUALS\" or \"STARTS\" or \"STARTED\" or \"FINISHES\" or \"FINISHED\" or \"MEETS\" or \"MET\" or \"OVERLAPS\" or \"OVERLAPPED\" or \"CONTAINS\" or \"CONTAINED\" or \"BEFORE\" or \"AFTER\" or \"GOVERLAPS\""  << endl;
    cerr << "       -o optimizations" << endl;
    cerr << "              set optimizations to be used: \"ss\" or \"1setup\" or \"1setup_cm\"; omit option for base HINT" << endl;
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       ./query_hint.exec -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry" << endl;
    cerr << "       ./query_hint.exec -o SS -q gOVERLAPS -v samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R;
    HierarchicalIndex *idxR;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    double totalIndexTime = 0, totalQueryTime = 0, querytime = 0, avgQueryTime = 0;
    Timestamp qstart, qend;
    RunSettings settings;
    char c;
    double vmDQ = 0, rssDQ = 0, vmI = 0, rssI = 0;
    string strPredicate = "", strOptimizations = "";

    
    // Parse command line input
    settings.init();
    settings.method = "hint";
    while ((c = getopt(argc, argv, "?hvq:o:r:")) != -1)
    {
        switch (c)
        {
            case '?':
            case 'h':
                usage();
                return 0;
                
            case 'v':
                settings.verbose = true;
                break;

            case 'q':
                strPredicate = toUpperCase((char*)optarg);
                break;
                
            case 'o':
                strOptimizations = toUpperCase((char*)optarg);
                break;

            case 'r':
                settings.numRuns = atoi(optarg);
                break;
                
            default:
                cerr << endl << "Error - unknown option '" << c << "'" << endl << endl;
                usage();
                return 1;
        }
    }
    
    
    // Sanity check
    if (argc-optind != 2)
    {
        usage();
        return 1;
    }
    if (!checkPredicate(strPredicate, settings))
    {
        if (strPredicate == "")
        cerr << endl << "Error - predicate type not defined" << endl << endl;
        else
        cerr << endl << "Error - unknown predicate type \"" << strPredicate << "\"" << endl << endl;
        usage();
        return 1;
    }
    if (!checkOptimizations(strOptimizations, settings))
    {
        cerr << endl << "Error - unknown optimizations type \"" << strOptimizations << "\"" << endl << endl;
        usage();
        return 1;
    }
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];
    
    
    // Load data and queries
    R.load(settings.dataFile);
    settings.maxBits = int(log2(R.gend-R.gstart)+1);
    
    ifstream fQ(settings.queryFile);
    if (!fQ)
    {
        cerr << endl << "Error - cannot open query file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }
    process_mem_usage(vmDQ, rssDQ);

    
    // Build index
    settings.numBits = settings.maxBits;
    
    if (settings.typeOptimizations == HINT_OPTIMIZATIONS_NO)
    {
        tim.start();
        idxR = new HINT(R, settings.numBits);
        totalIndexTime = tim.stop();
    }
    else if (settings.typeOptimizations == HINT_OPTIMIZATIONS_SS)
    {
        tim.start();
        idxR = new HINT_SS(R, settings.numBits);
        totalIndexTime = tim.stop();
    }
    process_mem_usage(vmI, rssI);

    
    // Execute queries
    size_t sumQ = 0;
    if (settings.verbose)
        cout << "Query\tPredicate\tMethod\tBits\tOptimizations\tResult\tTime" << endl;
    while (fQ >> qstart >> qend)
    {
        sumQ += qend-qstart;
        numQueries++;
        
        double sumT = 0;
        for (auto r = 0; r < settings.numRuns; r++)
        {
            switch (settings.typePredicate)
            {
                case PREDICATE_EQUALS:
                    tim.start();
                    queryresult = idxR->execute_Equals(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_STARTS:
                    tim.start();
                    queryresult = idxR->execute_Starts(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_STARTED:
                    tim.start();
                    queryresult = idxR->execute_Started(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_FINISHES:
                    tim.start();
                    queryresult = idxR->execute_Finishes(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_FINISHED:
                    tim.start();
                    queryresult = idxR->execute_Finished(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_MEETS:
                    tim.start();
                    queryresult = idxR->execute_Meets(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_MET:
                    tim.start();
                    queryresult = idxR->execute_Met(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_OVERLAPS:
                    tim.start();
                    queryresult = idxR->execute_Overlaps(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_OVERLAPPED:
                    tim.start();
                    queryresult = idxR->execute_Overlapped(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_CONTAINS:
                    tim.start();
                    queryresult = idxR->execute_Contains(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_CONTAINED:
                    tim.start();
                    queryresult = idxR->execute_Contained(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_PRECEDES:
                    tim.start();
                    queryresult = idxR->execute_Precedes(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_PRECEDED:
                    tim.start();
                    queryresult = idxR->execute_Preceded(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;

                case PREDICATE_GOVERLAPS:
                    tim.start();
                    queryresult = idxR->execute_gOverlaps(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                    break;
            }
            sumT += querytime;
            totalQueryTime += querytime;
            
            if (settings.verbose)
                 cout << "[" << qstart << "," << qend << "]\t" << strPredicate << "\t" << settings.method << "\t" << settings.numBits << "\t" << strOptimizations << "\t" << queryresult << "\t" << querytime << endl;
        }
        totalResult += queryresult;
        avgQueryTime += sumT/settings.numRuns;
    }
    fQ.close();
    
    
    // Report
    idxR->getStats();
    cout << endl;
    cout << "HINT" << endl;
    cout << "====" << endl;
    cout << "Input" << endl;
    cout << "  Num of intervals          : " << R.size() << endl;
    cout << "  Domain size               : " << (R.gend-R.gstart) << endl;
    cout << "  Avg interval extent [%]   : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Index" << endl;
    cout << "  Optimizations             : " << ((settings.typeOptimizations == HINT_OPTIMIZATIONS_NO)? "no": strOptimizations) << endl;
    cout << "  Num of bits               : " << settings.numBits << endl;
    cout << "  Num of partitions         : " << idxR->numPartitions << endl;
    cout << "  Num of Originals          : " << idxR->numOriginals << endl;
    cout << "  Num of replicas           : " << idxR->numReplicas << endl;
    cout << "  Num of empty partitions   : " << idxR->numEmptyPartitions << endl;
    printf( "  Avg partition size        : %f\n", idxR->avgPartitionSize);
    printf( "  Read VM [Bytes]           : %ld\n", (size_t)(vmI-vmDQ)*1024);
    printf( "  Read RSS [Bytes]          : %ld\n", (size_t)(rssI-rssDQ)*1024);
    printf( "  Indexing time [secs]      : %f\n\n", totalIndexTime);
    cout << endl;
    cout << "Queries" << endl;
    cout << "  Predicate type            : " << strPredicate << endl;
    cout << "  Num of runs per query     : " << settings.numRuns << endl;
    cout << "  Num of queries            : " << numQueries << endl;
    cout << "  Avg query extent [%]      : "; printf("%f\n", (((float)sumQ/numQueries)*100)/(R.gend-R.gstart));
    cout << "  Total result [";
#ifdef WORKLOAD_COUNT
    cout << "COUNT]      : ";
#else
    cout << "XOR]        : ";
#endif
    cout << totalResult << endl;
    printf( "  Total querying time [secs]: %f\n", totalQueryTime/settings.numRuns);
    printf( "  Avg querying time [secs]  : %f\n\n", avgQueryTime/numQueries);
    printf( "  Throughput [queries/sec]  : %f\n\n", numQueries/(totalQueryTime/settings.numRuns));
    
    delete idxR;
    
    
    return 0;
}
