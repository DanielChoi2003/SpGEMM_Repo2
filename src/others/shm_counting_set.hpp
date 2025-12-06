#include <ygm/comm.hpp>
#include <iostream>
#include <algorithm>
#include <cassert>
#include <ygm/detail/layout.hpp>
#include <sys/mman.h>   // For shm_open, mmap
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */

template <typename Key>
class shm_counting_set{

public:
    using self_type = shm_counting_set<Key>;


private:

  ygm::comm                           &m_comm;
  std::vector<std::pair<Key, int32_t>> m_count_cache;
  bool                                 m_cache_empty = true;
  internal_container_type              m_map;
};