#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/io/csv_parser.hpp>
#include <ygm/container/bag.hpp>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cassert>

using std::cout, std::endl;

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

struct metadata{
    int src = -1;
    int first_index = -1;
    int edge_count = 0;

    template <class Archive>
    void serialize( Archive & ar )
    {
        ar(src, first_index, edge_count);
    }

    bool operator<(const metadata& mt) const
    {
        if(src != mt.src){
            return src < mt.src;
        }
        else if(first_index != mt.first_index){
            return first_index < mt.first_index;
        }
        
        return edge_count < mt.edge_count;
    }
};

int getIndex(int source, std::vector<metadata> vec, int startIndex){
    // binary search
    auto it = std::lower_bound(vec.begin(), vec.end(), source, 
                            [](const metadata& mt, int src){
                                return mt.src < src;
                            });

    if(it != vec.end() && it->src == source){ return it - vec.begin(); }

    return -1;
}

using graph_type = ygm::container::array<metadata>;

// first adds edge by setting the index of the first occurring source, then increments the edge count to 1
// if not the first not, it only increments the edge count 
void update_edge(graph_type& graph, int src,
              int index) {

  auto updater = [](int src, metadata& mt, int index) {
    if(mt.first_index == -1){ // first occurrence
        mt.first_index = index;
    }
    mt.edge_count++;
  };

  graph.async_visit(src, updater, index); // creates an entry if it did not exist
}

int main(int argc, char** argv){
    
    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;

    /*
        int Edge_num = 0;
        if(world.rank0()){ // parse the csv header to get the number of Edges
        std::string header_file = "../data/matrix_data/testing-header.csv";
        std::ifstream header(header_file);

        if(!header.is_open()){
            s_world.cout0("Could not find file ", header_file);
            return -1;
        }
        std::string header_info;
        std::getline(header, header_info);
        int rows;
        int columns;
        int Edges;
        for(int i = 0; i < 3; i++){
            int index;
            if(i == 0){
                index = header_info.find(',');
                rows = std::stoi(header_info.substr(0, index));
                header_info = header_info.substr(index+1);
            }
            else if(i == 1){
                index = header_info.find(',');
                columns = std::stoi(header_info.substr(0, index));
                header_info = header_info.substr(index+1);
            }
            else{
                Edges = std::stoi(header_info);
            }
        }
        header.close();

        s_world.async_bcast([&](int number_of_Edges){
            Edge_num = number_of_Edges;
        }, Edges);
    }
        world.barrier();
        world.cout("Got number of Edges: ", Edge_num);
    */

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


    // Task 2: data storage and sharing among ranks
    /*
        How to split data among ranks?
        1. Using sorted Edge list for matrix A. 

            Use ygm::array, but the downside is that when it resizes, it needs to redo the partitioning.

        2. GLOBAL data structure is needed to know two things
            a. the index of the first source
            b. how many edges with that source (so I don't have to calculate how many times to iterate later)

            2.1: Should it be completely visible to all ranks? (need to be broadcasted)
            2.2: Or simply use distributed data structure ygm::map?
        
        3. (optional) Using sorted Edge list for matrix B. Must be tested whether this brings performance boost.

        Questions: 
            1. Is there a better way to partition csv data among ranks than inserting them into a bag then into an array?

            2. How does array globally sort? similar to merge sort?
                pivot sort -> prefix sum
            3. Currently, only rank 1 is performing the csv parse. Is it because due to byte fixed partitioning?
                answer: 8 MB chunk partitioning. heavy on system. 9MB -> 8MB, 1MB
            4. how to deallocate the bag containers?
                answer: use a scope
            5. what test method to confirm the correctness?
                create unit test cases in matlab or numpy
            6. how does csv_parser perform partition?
            7. async_visit_if_contain() seems to not work on ygm::array 
                no it does not. Used for ygm::map

    */
   size_t bag_size = bag_A.size();

    //world.cout0("bag size: ", bag_size);
    double abs_start = MPI_Wtime();
    YGM_ASSERT_RELEASE(bag_A.size() != 0);
    YGM_ASSERT_RELEASE(bag_B.size() != 0);
    ygm::container::array<Edge> matrix_A(world, bag_A);
    static ygm::container::array<Edge> &s_matrix_A = matrix_A;
    ygm::container::array<Edge> matrix_B(world, bag_B); // BOOKMARK: Sort matrix B later
    static ygm::container::array<Edge> &s_matrix_B = matrix_B;
    // deallocate bag_A and bag_B
    world.barrier();
    matrix_B.sort(); // Globally sort matrix B
    bag_A.clear(); // deallocates majority.
    bag_B.clear();
    double ext_end = MPI_Wtime(); 
    // TO-DO: find the global max time
    world.cout0("Extracting data took ", ext_end - abs_start, " seconds");

    /*

        vector data;
        data.clear();
        {
            vector newVec;
            data.swap(newVec);
        }

        //data.shrink_to_fit();
    */

    /*
        Perhaps have each rank perform only calculation on their own before sending the data to rank 0 to save time
    */
    /*
        use distributed map with async_visit
        (source/row number, key<index, edge_count)
        if it does not exist, then set the index and increment 
        if it does exist, then only increment the edge_count

        use local unordered_map to store these metadata
        use gather()
    */

    // matrix_B.for_all([](int index, Edge &ed){
    //     s_world.cout(ed.row, ", ", ed.col, ", ", ed.value);
    // });


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
    int num_of_processors = world.size();
    std::vector<std::pair<int, int>> metadata(num_of_processors);

    int local_size = matrix_B.local_size();
    int local_min = -1;
    int local_max = -1;
    auto minSetter = [&local_min](int index, Edge &ed){
        local_min = ed.row;
    };
    auto maxSetter = [&local_max](int index, Edge &ed){
        local_max = ed.row;
    };
    // local_start() global index of the rank's first local element
    // local_visit() expects a global index, used when that global index belongs to the called rank
    if(local_size != 0){
        matrix_B.local_visit(matrix_B.partitioner.local_start(), minSetter);
        matrix_B.local_visit(matrix_B.partitioner.local_start() + local_size - 1, maxSetter);
    }
    world.barrier();

    auto mt_inserter = [&metadata](int rank_num, std::pair<int, int> min_max){
        //printf("Inserting local min %d and local max %d at index %d\n", min_max.first, min_max.second, rank_num);
        metadata.at(rank_num) = min_max;
    };
    // gather does NOT work on ygm::array
    world.async(0, mt_inserter, world.rank(), std::make_pair(local_min, local_max));
    world.barrier();

    // now broadcast it to all other ranks
    auto broadcastMetadata = [&metadata](std::vector<std::pair<int, int>> incoming_metadata){
        metadata = incoming_metadata;
    };
    if(world.rank0()){
        world.async_bcast(broadcastMetadata, metadata);
    }
    world.barrier();
    
    if(world.rank() == 1){
        for(int i = 0; i < metadata.size(); i++){
            printf("rank %d: local min %d, local max %d\n", i, metadata.at(i).first, metadata.at(i).second);
        }
    }

    //world.cout(world.rank(), ": minimum row number = ", local_min, ", maximum row number, = ", local_max);

    //#define MAP_IMP
    #ifdef MAP_IMP
    ygm::container::map<int, metadata> metadata_map(world);
    static ygm::container::map<int, metadata> &s_metadata_map = metadata_map;
    matrix_B.for_all([](int index, Edge &ed){

        auto visitor = [](int source, metadata &mt, int index, Edge ed){
            if(mt.edge_count == 0 || mt.first_index > index){ // uninitialized
                mt.first_index = index;
            }
            mt.src = ed.row;
            mt.edge_count++;
        };

        s_metadata_map.async_visit(ed.row, visitor, index, ed);
    });
    world.barrier();
    
    static std::unordered_map<int, metadata> local_metadata;
    metadata_map.gather(local_metadata);
    #endif
    // potential race condition with std::vector
    // auto merger = [](std::vector<metadata> indiv_metadata){
    //     // rank 0 may be sending the same local data to itself (if merging it into local_metadata)
    //     global_metadata.insert(global_metadata.end(), indiv_metadata.begin(), indiv_metadata.end()); 
    // };
    // s_world.async(0, merger, local_metadata);


    world.barrier();

    // if(world.rank0()){
    //     for(auto &mt : local_metadata){
    //         printf("source/row number: %d, first index: %d, edge count: %d\n", mt.first, mt.second.first_index, mt.second.edge_count);
    //     }
    // }
    // world.cout0("---------------------------------------");
   
    #if 0
    double metadata_time = MPI_Wtime();
    world.cout0("Completed creating metadata vector in ", metadata_time - abs_start, " seconds");

    /*
        Task 3: matrix C data structure

        1. Naive implementation: use ygm::map
    */
    ygm::container::map<std::pair<int, int>, int> matrix_C(world);  // <row, col>, partial product 
    static ygm::container::map<std::pair<int, int>, int> &s_matrix_C = matrix_C;

    /*
        Task 4: perform outer product multiplication

        1. matrix B initiates the multiplication

        2. Find the rank that holds the first occurrence of the matching row (col B == row A)
        
    */


    // Question: using & vs static.
    static int mat_B_size = matrix_B.size();

    // matrix_A.for_all([](int index, Edge &ed){
    //     printf("row: %d, col: %d, value: %d\n", ed.row, ed.col, ed.value);
    // });

    double SpGEMM_start = MPI_Wtime();
    matrix_A.for_all([](int index, Edge &ed){
        int column_A = ed.col; // need a matching row (source)
        int row_A = ed.row;
        int value_A = ed.value;
        if(local_metadata.find(column_A) != local_metadata.end()){ // found a matching row in matrix A
            auto mt = local_metadata.find(column_A);
            int src = mt->first;
            int src_edge_count = mt->second.edge_count;
            int start_index = mt->second.first_index;

            auto multiplier = [](int index, Edge &ed, int value_A, int row_A, int column_A){
                int partial_product = value_A * ed.value; // valueB * valueA;
                if(column_A != ed.row){
                    printf("From matrix A(%d, %d) value %d, ", row_A, column_A ,value_A);
                    printf("From matrix B(%d, %d) value %d, ", ed.row, ed.col ,ed.value);
                    printf("Got partial product: %d\n", partial_product);
                }
                
                /*
                    Task 5: Storing the partial products
                    1. How to store partial products?
                        a. create a linked list off the same key
                        b. use mapped_reduce() if the key already exists (overwriting)
                */
               s_matrix_C.async_insert({row_A, ed.col}, 0);
               auto adder = [](std::pair<int, int> coord, int &partial_product, int value_add){
                    partial_product += value_add;
               };
                s_matrix_C.async_visit(std::make_pair(row_A, ed.col), adder, partial_product); // Boost's hasher complains if I use a struct
            };           
            for(int i = 0; i < src_edge_count; i++){
                YGM_ASSERT_RELEASE(start_index + i < mat_B_size);
                s_matrix_B.async_visit(start_index + i, multiplier, value_A, row_A, column_A); // async_visit_if_contains does not work??
            }
        }
    });
    world.barrier();
    double abs_end = MPI_Wtime();
    world.cout0("SpGEMM calculation took ", abs_end - SpGEMM_start, " seconds");
    world.cout0("Extraction + Sorting + Metadata + Matrix Multiplication ", abs_end - abs_start, " seconds");

    //#define MATRIX_OUTPUT
    #ifdef MATRIX_OUTPUT
    matrix_C.for_all([](std::pair<int, int> pair, int product){
        printf("%d, %d, %d\n", pair.first, pair.second, product);
    });

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
    
    #define TRIANGLE_COUNTING
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
    static ygm::container::array<Edge> &s_arr_matrix_C = arr_matrix_C;

    ygm::container::map<std::pair<int, int>, int> diagonal_matrix(world);  //
    static ygm::container::map<std::pair<int, int>, int> &s_diagonal_matrix = diagonal_matrix;

    double triangle_count_start = MPI_Wtime();
    arr_matrix_C.for_all([](int index, Edge &ed){
        int column_C = ed.col; // need a matching row (source)
        int row_C = ed.row;
        int value_C = ed.value;
        if(local_metadata.find(column_C) != local_metadata.end()){ // found a matching row in matrix A
            auto mt = local_metadata.find(column_C);
            int src = mt->first;
            int src_edge_count = mt->second.edge_count;
            int start_index = mt->second.first_index;

            auto multiplier = [](int index, Edge &ed, int value_C, int row_C, int column_C){

                auto adder = [](std::pair<int, int> coord, int &accum, int value_add){
                    // don't forget that for weighted graph, you need to only add one for triangle counting
                    accum += value_add;
                };
                // we are only concerned about the diagonal, thus the row of matrix C and column of matrix B must match
                if(row_C == ed.col){
                    int partial_product = value_C * ed.value; // valueB * valueA;
                    YGM_ASSERT_RELEASE(column_C == ed.row);
                    
                    /*
                        Task 5: Storing the partial products
                        1. How to store partial products?
                            a. create a linked list off the same key
                            b. use mapped_reduce() if the key already exists (overwriting)
                    */
                   
                    // race condition?
                    //s_diagonal_matrix.async_insert({row_C, ed.col}, 0);
                    s_diagonal_matrix.async_visit(std::make_pair(row_C, ed.col), adder, partial_product); // Boost's hasher complains if I use a struct
                }
                
            };           
            for(int i = 0; i < src_edge_count; i++){
                YGM_ASSERT_RELEASE(start_index + i < mat_B_size);
                s_matrix_B.async_visit(start_index + i, multiplier, value_C, row_C, column_C); // async_visit_if_contains does not work??
            }
        }
    });
    world.barrier();

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

    #endif


    return 0;
}



/*
    Errors encountered:
        1. segmentation fault
        Solution: creating a static object that refer to the ygm containers.
        2. Attempting to use an MPI routine after finalizing MPICH 
              what():   !m_in_process_receive_queue /g/g14/choi26/SpGEMM_Project/build/_deps/ygm-src/include/ygm/detail/comm.ipp:1433 
        Solution: static objects live until program exit and the world (ygm::comm) is destroyed before the containers are destroyed
                    so either make the original object non-static and create a copy that is static and pointing to the original object
                    such that the original object is destroyed before the communicator is destroyed.
        3. Fatal error in PMPI_Test: Message truncated, error stack:
            PMPI_Test(174)....: MPI_Test(request=0x84cc90, flag=0x7fffffff9f14, status=0x7fffffff9f20) failed
        
            occured when trying to merge all the vectors to rank 0
        Solution: silly mistake. Was sending the vector to rank every iteration

        4. rank 0 outputs -1 for all the src numbers.
        
        Solution: did not serialize src in ar(). oops...

        5. global_metadata has an out of bound access when performing matrix multiplication. 
            Look closely at the .at() functions

        Solution: didn't sort the global metadata...

            
*/