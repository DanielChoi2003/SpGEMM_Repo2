
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/set.hpp>
#include <ygm/io/csv_parser.hpp>
#include <ygm/container/bag.hpp>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cassert>
#include <vector>


int main(int argc, char **argv){


    ygm::comm world(&argc, &argv);
    if(world.rank0()){
        ygm::container::set<int> setA(world);
    }

    world.cout("rank ", world.rank(), " here");
}