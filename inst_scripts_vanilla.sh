set -e

export PROJECTPATH=$PWD

./configure --prefix=$PROJECTPATH/../vanilla_post_inst/  --exec-prefix=$PROJECTPATH/../vanilla_post_inst/ 

make uninstall -j
make clean -j

./configure --prefix=$PROJECTPATH/../vanilla_post_inst/  --exec-prefix=$PROJECTPATH/../vanilla_post_inst/ 


make -j
make install
