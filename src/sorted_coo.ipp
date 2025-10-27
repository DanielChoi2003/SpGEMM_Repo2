#include "sorted_coo.hpp"

using std::vector;
/*
    Member functions defined inside the class body are implicitly inline.
*/
inline vector<int> Sorted_COO::getOwners(int source){
    // binary search
    vector<int> owners;
    // finds the position of the first occurrence not less than (either equal or greater) the given value
    auto it = std::lower_bound(metadata.begin(), metadata.end(), source, 
                            [](const std::pair<int, int> min_max, int src){
                                return min_max.second < src;
                            });

    while(it != metadata.end() && (it->first <= source && it->second >= source)){ 
        int owner_rank = it - metadata.begin();
        owners.push_back(owner_rank);
        it++;
    }

    return owners;
}

auto multiplier = [](int input_value, int input_row, int input_column, auto pCOO, auto pmap){
         // find the first Edge with matching row to input_column with std::lower_bound
        int low = 0;
        int high = pCOO->local_size;
        int upper_bound = high;

        while (low < high) {
            int mid = low + (high - low) / 2;
            Edge mid_edge = pCOO->lc_sorted_matrix.at(mid);

            if (mid_edge.row < input_column) { // the edge with matching row has to be to the right of mid
                low = mid + 1;
            }
            else { // the first edge with matching row has to be to the left of mid
                high = mid;
            }
        }

        // keep multiplying with the next Edge until the row number no longer matches
        for(int i = low; i < upper_bound; i++){
            Edge match_edge = pCOO->lc_sorted_matrix.at(i);  
            if(match_edge.row != input_column){
                break;
            }

            int partial_product = input_value * match_edge.value; // valueB * valueA;
            
            // if(input_row == 5){
            //     world.cout("Inserting position row ", input_row, ", column ", match_edge.col, " with value ", partial_product);
            // }   
            pmap->async_insert({input_row, match_edge.col}, 0);
            auto adder = [](std::pair<int, int> coord, int &partial_product, int value_add){
                partial_product += value_add;
            };
            pmap->async_visit(std::make_pair(input_row, match_edge.col), adder, partial_product); // Boost's hasher complains if I use a struct
        }
    }; 


inline void Sorted_COO::async_visit_row(int input_column, int input_row, int input_value, auto pmap, F user_func){
        // NOTE: CAPTURING THE DISTRIBUTED CONTAINER BY REFERENCE MAY LEAD TO UNDEFINED BEHAVIOR 
        //     because the distributed container may not be in the same memory address from the remote rank (callee)'s 
        //     memory layout
    
    vector<int> owners = getOwners(input_column);
    for(int owner_rank : owners){
        // if(owner_rank == 0 && input_column == 1){
        //     world.cout("calling with row ", input_row, " and col ", input_column);
        // }
        assert(owner_rank >= 0 && owner_rank < world.size());
        world.async(owner_rank, multiplier, input_value, input_row, input_column, pthis, pmap); // async_visit_if_contains does not work??
    }
    
    //DO NOT CALL BARRIER HERE. PROCESSOR NEEDS TO BE ABLE TO RUN MULTIPLE TIMES.
}




template <class Matrix, class Accumulator>
inline void Sorted_COO::spGemm(Matrix &unsorted_matrix, Accumulator &partial_accum){

    ygm::ygm_ptr<Accumulator> pmap(&partial_accum);
    unsorted_matrix.local_for_all([&](int index, Edge &ed){
        int input_column = ed.col;
        int input_row = ed.row;
        int input_value = ed.value;
        //world.cout("Input column: ", input_column, ", input row: ", input_row, ", input_value: ", input_value);
        async_visit_row(input_column, input_row, input_value, pmap);
    });
}

inline void Sorted_COO::printMetadata(){
    for(int i = 0; i < metadata.size(); i++){
        world.cout("rank ", i, ": local min " , metadata.at(i).first, ", local max ", metadata.at(i).second);
    }
}



