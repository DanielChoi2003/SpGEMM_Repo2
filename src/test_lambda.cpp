#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <iostream>


int main(int argc, char** argv){

    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;

    if (world.rank() == 0) {
        volatile char skew[7]; (void)skew;
    }

    int local_data = world.rank();

    auto call_func = [&local_data](){
        s_world.cout("local data: ", local_data);        
    };


    world.async(0, call_func);
}