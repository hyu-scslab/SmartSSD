set -e

export PROJECTPATH=$PWD

./configure --prefix=$PROJECTPATH/../ndp_post_inst/  --exec-prefix=$PROJECTPATH/../ndp_post_inst/ CFLAGS=' -DSMARTSSD -DSMARTSSD_DBUF -DWSUL_CTID_TEST '

make uninstall -j
make clean -j

./configure --prefix=$PROJECTPATH/../ndp_post_inst/  --exec-prefix=$PROJECTPATH/../ndp_post_inst/ CFLAGS='-DSMARTSSD -DSMARTSSD_DBUF -DWSUL_CTID_TEST '


make -j
make install
