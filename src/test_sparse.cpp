#include "sorted_coo.hpp"
#include <ygm/io/csv_parser.hpp>


int main(int argc, char** argv){

    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;
    
    //#define UNDIRECTED_GRAPH

    std::string uni_filename = "../data/1000x1000.csv";

     // Task 1: data extraction
    ygm::container::bag<Edge> bag_A(world);
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
        bag_A.async_insert(ed);
    });
    world.barrier();

    // matrix B data extraction
    ygm::container::bag<Edge> bag_B(world);
    std::vector<std::string> filename_B = {uni_filename};
    std::fstream file_B(filename_B[0]);
    YGM_ASSERT_RELEASE(file_B.is_open() == true);
    file_B.close();
    ygm::io::csv_parser parser_B(world, filename_B);
    parser_B.for_all([&](ygm::io::detail::csv_line line){

        int row = line[0].as_integer();
        int col = line[1].as_integer();
        int value = 1;
        if(line.size() == 3){
            value = line[2].as_integer();
        }
        #ifdef UNDIRECTED_GRAPH
            Edge rev = {col, row, value};
            bag_B.async_insert(rev);
        #endif
        Edge ed = {row, col, value};
        bag_B.async_insert(ed);
    });
    world.barrier();


    Sorted_COO test_COO(world, bag_B);

   // test_COO.printMetadata();

    int source = 250;
    if(world.rank0()){
        std::vector<int> owners = test_COO.getOwners(source);
        for(int owner_rank : owners){
            world.cout("Owner ", owner_rank, " owns ", source);
        }
    }
   

    return 0;
}