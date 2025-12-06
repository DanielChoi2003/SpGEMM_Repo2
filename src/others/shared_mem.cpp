#include <ygm/comm.hpp>
#include <iostream>
#include <algorithm>
#include <cassert>
#include <ygm/detail/layout.hpp>
#include <sys/mman.h>   // For shm_open, mmap
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */

struct MMapDestructor{
    size_t size;

    MMapDestructor(size_t s = 0) : size(s){} // constructor

    // when unique pointer goes out of scope, it calls the 
    // deleter function
    void operator()(void* ptr) const{
        if(ptr != MAP_FAILED){
            munmap(ptr, size);
        }
    }
};

void filename_to_BIP(std::vector<std::unique_ptr<void, MMapDestructor>> &ptrs_vec, int local_id, std::string filename, int size){  
    // open a file descriptor to the shared file 
    int fd = shm_open(filename.c_str(), O_RDWR, 0666);
    if (fd == -1) {
        perror("Opening received shm failed");
        return;
    }

    void *mmap_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, 
                    MAP_SHARED, fd, 0);
    if(mmap_ptr == MAP_FAILED){
        perror("mapping to *shared* BIP failed\n");
        return;
    }
    close(fd);

    ptrs_vec.at(local_id) = std::unique_ptr<void, MMapDestructor>(mmap_ptr, MMapDestructor(size));
}

#define SIZE 128
int main(int argc, char **argv){
    ygm::comm world(&argc, &argv);
    static ygm::comm &s_world = world;

    {
        /*
            node_size():
                return m_node_size
            
            * comm_local is a communicator for processors that share the same memory (node)

            * m_local_id: rank of the processor within the local communicator

            * comm_node is grouped based on processors' local id of their own respective physical node
            Example:
                Global rank | Physical Node | local id
                0           | A             | 0
                1           | A             | 1
                2           | B             | 0
                3           | B             | 1

                rank 0 and 2 are grouped into the same communicator because
                they both have local id of 0
                
                rank 1 and 3 are grouped into the same communicator because 
                they both have local id of 1

        */
        int node_size(world.layout().node_size());
        world.cout("node size: ", node_size);
        // globally finds the minimum node size?
        world.barrier();

    }

    
    int local_size(world.layout().local_size());
    world.barrier();
    //world.cout("local size: ", local_size);
    

    int local_id = world.layout().local_id();
    int node_id = world.layout().node_id();
    world.cout("local id: ", local_id, ", node id: ", node_id);
    // each processor creates a shared memory file via shm_open
    std::string filename_s = "/BIP_" + std::to_string(world.rank());
    const char *filename_c = filename_s.c_str();

    int fd = shm_open(filename_c, O_CREAT | O_RDWR, 0666);
    if(fd == -1){
        perror("shm_open() failed\n");
        return -1;
    }

    if(ftruncate(fd, SIZE) == -1){
        perror("ftruncate() failed\n");
        return -1;
    }
    // fd is no longer needed
    close(fd);


    std::vector<std::unique_ptr<void, MMapDestructor>> ptrs_vec(world.layout().local_size());
    std::vector<std::string> BIP_filenames(world.layout().local_size());

    /*
        nl_to_rank() does not work the number of cores is odd? 

        example scenario: 7 cores, 0 to 6

        using formula: node id * local node size + local id
        for rank 3: 0 * 4 + 3 = 3

        for rank 4: 1 * 3 + 0 = 3

        there are overlapping rank number
    */
    // merge filenames to the local id 0 rank within the same physical node
    auto collect_filenames = [&BIP_filenames](int local_id, int node_id, int global_rank, std::string filename, int size){
        printf("Rank %d, Received filename %s from local rank %d, global rank %d\n", s_world.rank(), filename.c_str(), local_id, global_rank);
    
        BIP_filenames.at(local_id) = filename;
    };
    
    int local_rank_zero = world.rank() - local_id;
    // int global_rank = s_world.layout().nl_to_rank(node_id, local_id);
    world.async(local_rank_zero, collect_filenames, local_id, node_id, world.rank(), filename_s, SIZE);
    world.barrier();

     // broadcast the filenames (within the same node)
    auto broadcastBIP = [&BIP_filenames](std::vector<std::string> incoming_filenames){
        BIP_filenames = incoming_filenames;
    };
    if(world.rank() == local_rank_zero){
        for(int i = local_rank_zero + 1; i < local_rank_zero + local_size; i++){
            world.cout("Sharing filenames to rank ", i);
            world.async(i, broadcastBIP, BIP_filenames);
        }
    }
    world.barrier();

    // if(world.rank() == 4){
    //     for(auto &filename : BIP_filenames){
    //         world.cout(filename);
    //     }
    // }
    
    // translate the filenames into pointers
    for(int i = 0; i < BIP_filenames.size(); i++){
        filename_to_BIP(ptrs_vec, i, BIP_filenames[i], SIZE);
    }

    if(world.rank() == local_rank_zero){
        std::string message = "Hello, this is rank " + std::to_string(world.rank());
        world.cout("Writing message: ", message);
        char* bip_ptr = (char*)ptrs_vec[0].get();
        std::memcpy(bip_ptr, message.data(), message.size());
    }
    world.barrier();

    char* message = (char*)ptrs_vec[0].get();
    printf("Rank %d read message: %s\n", world.rank(), message);


    // some processes may unlink before others get the chance to shm_open
    world.barrier();
    shm_unlink(filename_c);
}