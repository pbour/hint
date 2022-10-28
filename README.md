# HINT: A Hierarchical Index for Intervals in Main Memory

Source code for the following publications:
- George Christodoulou, Panagiotis Bouros, Nikos Mamoulis: HINT: A Hierarchical Index for Intervals in Main Memory. SIGMOD Conference 2022: 1257-1270, https://doi.org/10.1145/3514221.3517873
- 

## Dependencies
- g++/gcc
- Boost Library 


## Data

Directory  ```data``` includes the BOOKS dataset used in the experiments and a query file containing 20k queries 
- AARHUS-BOOKS_2013.dat
- AARHUS-BOOKS_2013.qry


## Compile
Compile using ```make all``` or ```make <option>``` where <option> can be one of the following:
   - lscan 
   - 1dgrid 
   - hint
   - hint_m 

## Shared parameters among all indexing methods
| Parameter | Description | Comment |
| ------ | ------ | ------ |
| -? or -h | display help message | |
| -v | activate verbose mode; print the trace for every query; otherwise only the final report | |
| -q | set predicate type: "EQUALS" or "STARTS" or "STARTED" or "FINISHES" or "FINISHED" or "MEETS" or "MET" or "OVERLAPS" or "OVERLAPPED" or "CONTAINS" or "CONTAINED" or "BEFORE" or "AFTER" or "GOVERLAPS" | |
| -r | set the number of runs per query; by default 1 |  |

## Indexing and query processing methods

### Linear scan:

#### Source code files
- main_lscan.cpp
- containers/relation.h
- containers/relation.cpp

- ##### Examples

    ```sh
    $ ./query_lscan.exec -q gOVERLAPS -r 10 data/AARHUS-BOOKS_2013.dat data/AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_lscan.exec -q gOVERLAPS -v data/AARHUS-BOOKS_2013.dat data/AARHUS-BOOKS_2013_20k.qry
    ```


### 1D-grid: 

#### Source code files
- main_1dgrid.cpp
- indices/1dgrid.h
- indices/1dgrid.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -p | set the number of partitions | 500 for BOOKS in the experiments |

- ##### Examples

    ```sh
    $ ./query_1dgrid.exec -p 100 -q gOVERLAPS -r 10 AARHUS-BOOKS_2013.dat AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_1dgrid.exec -p 300 -q gOVERLAPS -v AARHUS-BOOKS_2013.dat AARHUS-BOOKS_2013_20k.qry
    ```


### HINT: 

#### Source code files
- main_hint.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint.h
- indices/hint.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -o | activate the skewness & sparsity optimization |  |

- ##### Examples    

    ```sh
    $ ./query_hint.exec -q gOVERLAPS -r 10 data/AARHUS-BOOKS_2013.dat data/AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_hint.exec -o SS -q gOVERLAPS -v data/AARHUS-BOOKS_2013.dat data/AARHUS-BOOKS_2013_20k.qry
    ```


### HINT<sup>m</sup>: 

#### Source code files
- main_hint_m.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint_m.h
- indices/hint_m.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -b |  set the number of bits | 10 for BOOKS in the experiments |
| -o |  set optimizations to be used: "subs+sort" or "subs+sopt" or "subs+sort+sopt" or "subs+sort+sopt+ss" or "subs+sort+sopt+cm" or "all" or "subs+sort+ss+cm"| omit parameter for base HINT<sup>m</sup> |
| -t |  evaluate query traversing the hierarchy in a top-down fashion; by default the bottom-up approach is used | currently supported only by base HINT<sup>m</sup> |

- ##### Stabbing query    

    ```sh
    $ ./query_hint_m.exec -b 10 -o all -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ###### base with top-down
    ```sh
    $ ./query_hint_m.exec -t -b 10 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### base with bottom-up
    ```sh
    $ ./query_hint_m.exec -b 10 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sopt -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt+ss (only bottom-up, "ss" -> sparsity & skewness optimization)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt+ss -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt+cm (only bottom-up, "cm" -> cache misses optimization)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt+cm -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### all optimizations  (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o all -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
