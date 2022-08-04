set -e

export PROJECTPATH=$PWD

./configure --prefix=$PROJECTPATH/../vanilla_post_inst/  --exec-prefix=$PROJECTPATH/../vanilla_post_inst/ 

make uninstall -j
make clean -j

./configure --prefix=$PROJECTPATH/../vanilla_post_inst/  --exec-prefix=$PROJECTPATH/../vanilla_post_inst/ --enable-coverage --enable-cassert --enable-debug CFLAGS='-O0 -g -rdynamic'


make -j
make install
