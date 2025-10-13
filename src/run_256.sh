factor=2
constant=256
srun -n "$constant" -t 30:00 -ppbatch -A coda \
    ./src/sparse &> "com-lj.ungraph.txt"