#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/io/csv_parser.hpp>
#include <iostream>
#include "sorted_coo.hpp"



int main(int argc, char **argv){

    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;

    std::string uni_filename = "../data/10x10.csv";

     // Task 1: data extraction
    auto bagap = std::make_unique<ygm::container::bag<Edge>>(world);
    std::vector<std::string> filename_A = {uni_filename};
    std::fstream file_A(filename_A[0]);
    YGM_ASSERT_RELEASE(file_A.is_open() == true);
    file_A.close();
    ygm::io::csv_parser parser_A(world, filename_A);
    parser_A.for_all([&](ygm::io::detail::csv_line line){ // currently rank 0 is the only one running. is byte partition fixed?

        int row = line[0].as_integer();
        int col = line[1].as_integer();
        int value = 1;
        if(line.size() == 3){
           value = line[2].as_integer();
        }
        #ifdef UNDIRECTED_GRAPH
            Edge rev = {col, row, value};
            bag_A.async_insert(rev);
        #endif
        Edge ed = {row, col, value};
        bagap->async_insert(ed);
    });
    world.barrier();

    ygm::container::array<Edge> copyArr(world, *bagap);
    bagap.reset();

    copyArr.for_all([](int index, Edge &ed){
        s_world.cout("row: ", ed.row, ", col: ", ed.col, ", value: ", ed.value);
    });
    

    return 0;
}