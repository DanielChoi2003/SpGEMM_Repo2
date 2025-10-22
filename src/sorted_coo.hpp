#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/io/csv_parser.hpp>
#include <ygm/container/bag.hpp>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cassert>
#include <vector>

using map_index = std::pair<int, int>;

struct Edge{
    int row;
    int col;
    int value;
    bool operator<(const Edge& B) const{ // does not modify the content
        if (row != B.row) return row < B.row; // first, sort by row
        if (col != B.col) return col < B.col; // if rows are equal, sort by column
        return value < B.value; // lastly sort by value
    }

    template <class Archive>
    void serialize( Archive & ar )
    {
        ar(row, col, value);
    }
};


class Sorted_COO{

public:

    /*
        @brief Initializes the ygm::container::array member with a ygm::container::bag provided by the user.

        @param ygm::comm&: communicator object
        @param ygm::container::array<Edge>& src: already sorted array
    */
    explicit Sorted_COO(ygm::comm& c, ygm::container::array<Edge>& src): world(c), pthis(this) {

        pthis.check(world);
        /*
            index = rank number
            pair<minimum row number, maximum row number> the rank holds
            get the minimum and maximum row number that each processor holds

            to gather/merge, you can either use:
            1. a distributed data structure, then call gather on it
            2. use rank 0's local data structure, call async to rank 0, 
                insert the data into index that matches the caller rank's id.
                Then rank 0 broadcasts to all other ranks
        */
       // does comm::size() implicitly call barrier()?
       // array.size() contains a barrier()
        int num_of_processors = world.size();
        metadata.resize(num_of_processors);

        src.local_for_all([this](int index, Edge ed){
            lc_sorted_matrix.push_back(ed);
        });

        global_start = src.partitioner.local_start();
        global_end = global_start + src.local_size();
        local_min = lc_sorted_matrix.front().row;
        local_max = lc_sorted_matrix.back().row;

        //printf("rank %d: local min %d, local max %d\n", world.rank(), local_min, local_max);
        auto mt_inserter = [](int rank_num, std::pair<int, int> min_max, auto pCoo){
            //printf("Inserting local min %d and local max %d at index %d\n", min_max.first, min_max.second, rank_num);
            pCoo->metadata.at(rank_num) = min_max;
        };
        // gather does NOT work on ygm::array. does work on map
        world.async(0, mt_inserter, world.rank(), std::make_pair(local_min, local_max), pthis);
        world.barrier();

        //now broadcast it to all other ranks
        auto broadcastMetadata = [this](std::vector<std::pair<int, int>> incoming_metadata, auto pCOO){
            pCOO->metadata = incoming_metadata;
        };

        if(world.rank0()){
            world.async_bcast(broadcastMetadata, metadata, pthis);
        }


        world.barrier(); 
    }

    // template <typename YGMContainer>
    //     map(ygm::comm&          comm,
    //         const YGMContainer& yc) requires detail::HasForAll<YGMContainer> &&
    //         detail::SingleItemTuple<typename YGMContainer::for_all_args>
    //         : m_comm(comm), pthis(this), partitioner(comm), m_default_value() {
    //         m_comm.log(log_level::info, "Creating ygm::container::map");
    //         pthis.check(m_comm);

    //         yc.for_all([this](const std::pair<Key, Value>& value) {
    //         this->async_insert(value);
    //         });

    //         m_comm.barrier();
    //     }


    /*
        @brief 
            prints each rank's metadata vector. A test case function to ensure that 
            each rank contains the same global data.
    */
    void printMetadata();


    /*
        @brief 
            gets the owners of the row number that matches to the given argument "source".
    
        @param source: the number of the row number 
    */
    std::vector<int> getOwners(int source);

    /*
        prints all elements of the sorted matrix
    */
   void printAll();
   
    /*
        @brief
            finds the set of owners (ranks) that contains elements with the matching row number.
            The caller of this function calls the owner(s) by providing the column number, row number, and
            value operands to multiply with.
            The callee will find the index of the first occurring element with a matching row number.
            The callee will multiply the found elements with the given value and store the partial products in
            [given row number, the multiplied element's column number].



        @param input_column: incoming column number. Will be multipled with a value that has a matching row number.
        @param input_row: incoming number row number. Used to determine the partial product's index.
        @param input_value: what will be multiplied with.

        @return none
    */
    void async_visit_row(int input_column, int input_row, int input_value, auto pmap);


    /*
        @brief 
            Matrix A (unsorted) starts the matrix multiplication. Intermediate partial products are stored
            in the Accumulator class, which is a ygm::container::map for now.
            This function calls async_visit_row();

        @param Matrix matrix_A: unsorted matrix that starts the sparse multiplication. Traverses column-by-column.
        @param Accumulator C: distributed map that stores the partial products
    */
    template <class Matrix, class Accumulator>
    void spGemm(Matrix &matrix_A, Accumulator &partial_accum);


private:
    /*
        contains each processor's min and max source number (row number)
    */
    std::vector<std::pair<int, int>> metadata;

    int global_start = -1;
    int global_end = -1;
    int local_min = -1;
    int local_max = -1;
    ygm::comm &world;                            // store the communicator. Hence the &
    std::vector<Edge> lc_sorted_matrix;  // store the local sorted matrix
    typename ygm::ygm_ptr<Sorted_COO> pthis;
};


// including the ipp file here removes the need to add it in add_ygm_executable()
#include "sorted_coo.ipp"


/*
    1. would having another YGM container in the class lead to too much overhead? Does it create an entirely new copy
        or use the local data to create a partial copy. Cannot determine the behavior of multiple ranks calling the same
        constructor function.

    2. When using lambda function, does captured variable always refer to the callee's or caller's?
        Answer:
            Assuming that & uses the caller's memory address

    3. using "this" pointer leads to segmentation fault.
        Theory is that the memory address contained in "this" pointer may be different from the callee's "this" pointer's memory
        address, thus leading to segmentation fault.
    
    
    
    undefined reference to sorted_coo.ipp. 
        Solution: adding inline to defined functions and adding #include "sorted_coo.ipp" at the end of "sorted_coo.hpp"

*/
