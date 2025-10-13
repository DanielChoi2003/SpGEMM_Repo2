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

    while(it != metadata.end() && (it->first == source || it->second == source)){ 
        int owner_rank = it - metadata.begin();
        owners.push_back(owner_rank);
        it++;
    }

    return owners;
}

inline void Sorted_COO::async_visit_row(int input_column, int input_row, int input_value, ygm::container::map<map_index, int> &matrix_C){
    
    // find the owners of the matching row / source number.


}

inline void Sorted_COO::printMetadata(){
    for(int i = 0; i < metadata.size(); i++){
        world.cout("rank ", i, ": local min " , metadata.at(i).first, ", local max ", metadata.at(i).second);
    }
}

