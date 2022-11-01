# HINT: A Hierarchical Index for Intervals in Main Memory

Source code from the following publications:
- George Christodoulou, Panagiotis Bouros and Nikos Mamoulis, <i>HINT: A Hierarchical Interval Index for Allen Relationships</i>, under review
- George Christodoulou, Panagiotis Bouros and Nikos Mamoulis, <i>HINT: A Hierarchical Index for Intervals in Main Memory</i>, https://doi.org/10.1145/3514221.3517873, ACM SIGMOD Conference, pp. 1257-1270 (2022)

## Dependencies
- g++/gcc
- Boost Library 


## Data

Directory  ```samples``` includes the BOOKS dataset used in the experiments and a query file containing 20k queries 
- AARHUS-BOOKS_2013.dat
- AARHUS-BOOKS_2013.qry


## Compile
Compile using ```make all``` or ```make <option>``` where `<option>` can be one of the following:
   - lscan 
   - 1dgrid 
   - hint
   - hint_m 


## Shared parameters among all methods
| Parameter | Description | Comment |
| ------ | ------ | ------ |
| -? or -h | display help message | |
| -v | activate verbose mode; print the trace for every query; otherwise only the final report is displayed | |
| -q | set predicate type:<br>(1) basic relationships from Allen's algebra, "EQUALS", "STARTS", "STARTED", "FINISHES", "FINISHED", "MEETS", "MET", "OVERLAPS", "OVERLAPPED", "CONTAINS", "CONTAINED", "BEFORE" "AFTER"<br>(2) generalized overlaps, "gOVERLAPS", from ACM SIGMOD'22 publication  | basic predicates work only the linear scan method, 1D-grid and for the most advanced HINT<sup>m</sup> variants, with SUBS+SORT+SS+CM or ALL optimizations |
| -r | set the number of runs per query; by default 1 |  |


## Workloads
The code supports two types of workload:
- Counting the qualifying records, or
- XOR'ing between their ids

You can switch between the two by appropriately setting the `WORKLOAD_COUNT` flag in def_global.h; remember to use `make clean` after resetting the flag. 


## Indexing and query processing methods

### Linear scan:

#### Source code files
- main_lscan.cpp
- containers/relation.h
- containers/relation.cpp

- ##### Examples

    ```sh
    $ ./query_lscan.exec -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_lscan.exec -q gOVERLAPS -v samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```


### 1D-grid: 

#### Source code files
- main_1dgrid.cpp
- containers/relation.h
- containers/relation.cpp
- indices/1dgrid.h
- indices/1dgrid.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -p | set the number of partitions | 500 for BOOKS in the experiments |

- ##### Examples

    ```sh
    $ ./query_1dgrid.exec -p 500 -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_1dgrid.exec -p 500 -q gOVERLAPS -v samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```


### HINT: 

#### Source code files
- main_hint.cpp
- containers/relation.h
- containers/relation.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint.h
- indices/hint.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -o | \"SS\" to activate the skewness & sparsity optimization |  |

- ##### Examples    

    ```sh
    $ ./query_hint.exec -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ```sh
    $ ./query_hint.exec -o SS -q gOVERLAPS -v samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```


### HINT<sup>m</sup>: 

#### Source code files
- main_hint_m.cpp
- containers/relation.h
- containers/relation.cpp
- containers/offsets.h
- containers/offsets.cpp
- containers/offsets_templates.h
- containers/offsets_templates.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint_m.h
- indices/hint_m.cpp
- indices/hint_m_subs+sort.cpp
- indices/hint_m_subs+sopt.cpp
- indices/hint_m_subs+sort+sopt.cpp
- indices/hint_m_subs+sort+sopt+ss.cpp
- indices/hint_m_subs+sort+sopt+cm.cpp
- indices/hint_m_subs+sort+ss+cm.cpp
- indices/hint_m_all.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -m |  set the number of bits | 10 for BOOKS in the experiments |
| -o |  set optimizations to be used: "SUBS+SORT" or "SUBS+SOPT" or "SUBS+SORT+SOPT" or "SUBS+SORT+SOPT+SS" or "SUBS+SORT+SOPT+CM" or "SUBS+SORT+SS+CM" or "ALL"| omit parameter for base HINT<sup>m</sup>; "CM" for cache misses optimization |
| -t |  evaluate query traversing the hierarchy in a top-down fashion; by default the bottom-up strategy is used | currently supported only by base HINT<sup>m</sup> |

- ##### Examples

    ###### base with top-down
    ```sh
    $ ./query_hint_m.exec -m 10 -t -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### base with bottom-up
    ```sh
    $ ./query_hint_m.exec -m 10 -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sort (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sort -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sopt -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sort+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sort+sopt -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sort+sopt+ss (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sort+sopt+ss -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sort+sopt+cm (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sort+sopt+cm -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### subs+sort+ss+cm optimizations  (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o subs+sort+sopt+cm -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```
    ###### all optimizations  (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -m 10 -o all -q gOVERLAPS -r 10 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry
    ```


## Notes / TODOs
The following are missing from the current version of the code:
- Model for automatically setting parameter "m" on HINT<sup>m</sup>
- HINT with SS optimization answering the basic predicates from Allen's algebra
- Updates 
