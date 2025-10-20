#include "sorted_coo.hpp"
#include <ygm/io/csv_parser.hpp>
#include <stdio.h>
#include <mpi.h>


int main(int argc, char** argv){

    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;
    
    #define UNDIRECTED_GRAPH

    std::string uni_filename = "../data/facebook_combined.csv";

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

    ygm::container::array<Edge> unsorted_matrix(world, bag_B);
    Sorted_COO test_COO(world, bag_B);


    int rank, size;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int namelen;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(processor_name, &namelen);

    printf("Hello from process %d of %d on node %s\n", rank, size, processor_name);
    
    if(world.rank0()){
        test_COO.printMetadata();  
    }

    int source = 1;
    if(world.rank0()){
        std::vector<int> owners = test_COO.getOwners(source);
        for(int owner_rank : owners){
            world.cout("Owner ", owner_rank, " owns ", source);
        }
    }
    ygm::container::map<std::pair<int, int>, int> matrix_C(world); 
    test_COO.spgemm(unsorted_matrix, matrix_C);

    world.barrier();
    //#define MATRIX_OUTPUT
    #ifdef MATRIX_OUTPUT
    // matrix_C.for_all([](std::pair<int, int> pair, int product){
    //     printf("%d, %d, %d\n", pair.first, pair.second, product);
    // });

    ygm::container::bag<Edge> global_bag_C(world);
    matrix_C.for_all([&global_bag_C](std::pair<int, int> coord, int product){
        global_bag_C.async_insert({coord.first, coord.second, product});
    });
    world.barrier();

    std::vector<Edge> sorted_output_C;
    global_bag_C.gather(sorted_output_C, 0);
    if(world.rank0()){
        std::sort(sorted_output_C.begin(), sorted_output_C.end());
        for(Edge &ed : sorted_output_C){
            printf("%d, %d, %d\n", ed.row, ed.col, ed.value);
        }
    }
    #endif


    //#define TRIANGLE_COUNTING
    #ifdef TRIANGLE_COUNTING
    double bag_C_start = MPI_Wtime();
    ygm::container::bag<Edge> bag_C(world);
    matrix_C.for_all([&bag_C](std::pair<int, int> indices, int value){
        bag_C.async_insert({indices.first, indices.second, value});
    });
    world.barrier();
    double bag_C_end = MPI_Wtime();
    world.cout0("Constructing bag C from map matrix C took ", bag_C_end - bag_C_start, " seconds");
    ygm::container::array<Edge> arr_matrix_C(world, bag_C);  // <row, col>, partial product 

    ygm::container::map<std::pair<int, int>, int> diagonal_matrix(world);  //

    double triangle_count_start = MPI_Wtime();
    test_COO.spgemm(arr_matrix_C, diagonal_matrix);
    world.barrier();

    // diagonal_matrix.for_all([](std::pair<int, int> pair, int product){
    //     if(pair.first == pair.second){
    //         printf("%d, %d, %d\n", pair.first, pair.second, product);
    //     }
    // });

    int triangle_count = 0;
    int global_triangle_count = 0;
    diagonal_matrix.for_all([&triangle_count](std::pair<int, int> indices, int value){
        if(indices.first == indices.second){
            triangle_count += value;
        }
    });
    world.barrier();

    auto adder = [&global_triangle_count](int value){
        global_triangle_count += value;
    };
    world.async(0, adder, triangle_count);
    world.barrier();

    double triangle_count_end = MPI_Wtime();
    world.cout0("Triangle counting and convergence took ", triangle_count_end - triangle_count_start, " seconds");
    if(world.rank0()){
        s_world.cout0("triangle count: ", global_triangle_count / 6);
    }
    #endif

   

    return 0;
}