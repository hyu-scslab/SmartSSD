set -e

export PROJECTPATH=$PWD

./configure --prefix=$PROJECTPATH/../ndp_post_inst/  --exec-prefix=$PROJECTPATH/../ndp_post_inst/ CFLAGS=' -DSMARTSSD -DSMARTSSD_DBUF -DSMARTSSD_DBUF_DEBUG -DWSUL_CTID_TEST '

make uninstall -j
make clean -j

./configure --prefix=$PROJECTPATH/../ndp_post_inst/  --exec-prefix=$PROJECTPATH/../ndp_post_inst/ --enable-coverage --enable-cassert --enable-debug CFLAGS='-O0 -DSMARTSSD -DSMARTSSD_DBUF -DSMARTSSD_DBUF_DEBUG -DWSUL_CTID_TEST -g -rdynamic'


make -j
make install
