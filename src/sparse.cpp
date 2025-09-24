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
    int src = -69;
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
        // long long vertex_one = std::min(vertex_a, vertex_b);
        // long long vertex_two = std::max(vertex_a, vertex_b);
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
        // long long vertex_one = std::min(vertex_a, vertex_b);
        // long long vertex_two = std::max(vertex_a, vertex_b);
        if(line.size() == 3){
            value = line[2].as_integer();
        }
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
    static std::vector<metadata> mat_metadata;
    ygm::container::array<Edge> matrix_A(world, bag_A);
    static ygm::container::array<Edge> &s_matrix_A = matrix_A;
    ygm::container::array<Edge> matrix_B(world, bag_B); // BOOKMARK: Sort matrix B later
    static ygm::container::array<Edge> &s_matrix_B = matrix_B;
    // deallocate bag_A and bag_B
    world.barrier();
    matrix_B.sort(); // Globally sort matrix A
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
    static std::vector<metadata> local_metadata;
    static std::vector<metadata> global_metadata;
    int cur_pos = 0;
    int respective_index = 0;
    matrix_B.for_all([&cur_pos](int index, Edge &ed){
        if(local_metadata.empty()){
            local_metadata.push_back({ed.row, index, 1});
        }
        else if(local_metadata.at(cur_pos).src != ed.row){
            local_metadata.push_back({ed.row, index, 1});
            cur_pos++;
        }
        else{ // the row number matches
            local_metadata.at(cur_pos).edge_count++;
        }
    });
    world.barrier();

    ygm::container::bag<metadata> metadata_bag(world);
    for(metadata md : local_metadata){
        metadata_bag.async_insert(md);
    }
    
    // potential race condition with std::vector
    auto merger = [](std::vector<metadata> indiv_metadata){
        // rank 0 may be sending the same local data to itself (if merging it into local_metadata)
        global_metadata.insert(global_metadata.end(), indiv_metadata.begin(), indiv_metadata.end()); 
    };
    s_world.async(0, merger, local_metadata);
    world.barrier();

    // if(world.rank0()){
    //     for(auto &mt : global_metadata){
    //         printf("source/row number: %d, first index: %d, edge count: %d\n", mt.src, mt.first_index, mt.edge_count);
    //     }
    // }
    // world.cout0("---------------------------------------");
    if(world.rank0()){
        std::sort(global_metadata.begin(), global_metadata.end());

        // merge the same source information
        int first_index = 0;
        int current_src = -1; // assuming there is no source of -1
        for(size_t i = 0; i < global_metadata.size(); ){
            if(current_src == -1 || current_src != global_metadata.at(i).src){ 
                current_src = global_metadata.at(i).src;
                first_index = i;
                i++; // only move onto the next element if we did not erase an element
            }
            else if(current_src == global_metadata.at(i).src){
                global_metadata.at(first_index).edge_count += global_metadata.at(i).edge_count;
                global_metadata.erase(global_metadata.begin() + i);
            }
        }
        s_world.async_bcast([](std::vector<metadata> sorted_metadata){
            global_metadata = sorted_metadata;
        }, global_metadata);
    }
    world.barrier();
    double metadata_time = MPI_Wtime();
    world.cout0("Completed creating metadata vector in ", metadata_time - abs_start, " seconds");

    // if(world.rank() == 1){
    //     for(auto &mt : global_metadata){
    //         printf("source/row number: %d, first index: %d, edge count: %d\n", mt.src, mt.first_index, mt.edge_count);
    //     }
    // }
        

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
        // but what if there is no matching row?
        int row_A = ed.row;
        int value_A = ed.value;
        int rowB_index = -1;
        if((rowB_index = getIndex(column_A, global_metadata, 0)) != -1){ // found a matching row in matrix A
            int src = global_metadata.at(rowB_index).src;
            int src_edge_count = global_metadata.at(rowB_index).edge_count;
            int start_index = global_metadata.at(rowB_index).first_index;

            auto multiplier = [&column_A](int index, Edge &ed, int value_A, int row_A){
                int partial_product = value_A * ed.value; // valueB * valueA;
                // printf("From matrix A(%d, %d) value %d, ", row_A, column_A ,value_A);
                // printf("From matrix B(%d, %d) value %d, ", ed.row, ed.col ,ed.value);
                // printf("Got partial product: %d\n", partial_product);
                
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
                if(start_index + i >= mat_B_size){
                    cout << "src: " << src << ", edge_count: " << src_edge_count << endl;
                    return;
                }
                s_matrix_B.async_visit(start_index + i, multiplier, value_A, row_A); // async_visit_if_contains does not work??
            }
        }
    });
    world.barrier();
    double abs_end = MPI_Wtime();
    world.cout0("SpGEMM calculation took ", abs_end - SpGEMM_start, " seconds");
    world.cout0("Extraction + Sorting + Metadata + Matrix Multiplication ", abs_end - abs_start, " seconds");

    // matrix_C.for_all([](std::pair<int, int> pair, int product){
    //     printf("%d, %d, %d\n", pair.first, pair.second, product);
    // });

    ygm::container::bag<Edge> global_bag_C(world);
    matrix_C.for_all([&global_bag_C](std::pair<int, int> coord, int product){
        global_bag_C.async_insert({coord.first, coord.second, product});
    });
    world.barrier();

    std::vector<Edge> sorted_output_C;
    //size_t bag_size_C = global_bag_C.size();
    //world.cout0("bag size: ", bag_size_C);

    global_bag_C.for_all([&sorted_output_C](Edge &ed){
        sorted_output_C.push_back(ed);
    });
    world.barrier();

    {
        auto merger = [&sorted_output_C](std::vector<Edge> indiv_vector){
            // rank 0 may be sending the same local data to itself (if merging it into local_metadata)
            sorted_output_C.insert(sorted_output_C.end(), indiv_vector.begin(), indiv_vector.end()); 
        };
        s_world.async(0, merger, sorted_output_C);

    }
    world.barrier();
    

    // if(world.rank0()){
    //     std::sort(sorted_output_C.begin(), sorted_output_C.end());
    //     for(Edge &ed : sorted_output_C){
    //         printf("%d, %d, %d\n", ed.row, ed.col, ed.value);
    //     }
    // }



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