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
| -v | activate verbose mode to print out for every query or update | |
| -q | set query type "stabbing" or "range" | Only for querying |
| -r | set the number of runs per query; by default 1 | Only for querying |

## Indexing methods

### Linear scan:

#### Source code files
- main_lscan.cpp
- containers/relation.h
- containers/relation.cpp

- ##### Range query

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

- ##### Stabbing query    

    ```sh
    $ ./query_1dgrid.exec -p 500 -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_1dgrid.exec -p 500 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_1dgrid.exec -p 500 -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
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
| -o | activate the skewness & sparsity optimization |  Only for querying |

- ##### Stabbing query    

    ```sh
    $ ./query_hint.exec -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ```sh
    $ ./query_hint.exec -o -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_hint.exec -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ```sh
    $ ./query_hint.exec -o -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_hint.exec -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
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
| -t  |  activate the top-down evaluation approach |  Available only for base HINT<sup>m</sup> and the range query; omit option for bottom-up |
| -o |  set optimizations to be used: "subs+sort" or "subs+sopt" or "subs+sort+sopt" or "subs+sort+sopt+ss" or "subs+sort+sopt+cm" or "all"| Omit option for base HINT<sup>m</sup> |

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

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions (only bottom-up)

    ```sh
    $ ./update_hint_m.exec -b 10 -q range -o subs+sopt samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```

    ```sh
    $ ./update_hint_m.exec -b 10 -q range -o all samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```

