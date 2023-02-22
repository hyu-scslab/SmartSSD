
IROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $IROOTDIR

./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --no-ndp 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=3

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --ndp --include-ndp-library 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=3 --ndp

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --no-ndp 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=8

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --ndp --include-ndp-library 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/

./run_query.sh -w=100 -q=8 --ndp

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --no-ndp 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=9

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --ndp --include-ndp-library 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=9 --ndp

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --no-ndp 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=17

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --ndp --include-ndp-library 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=17 --ndp

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --no-ndp 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=19

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

./install_script_with_pghint.sh --ndp --include-ndp-library 
cd $IROOTDIR

sleep 120
./run_server.sh 
cd $IROOTDIR

cd $IROOTDIR/../../ch-benchmark/
./run_query.sh -w=100 -q=19 --ndp

cd $IROOTDIR
./shutdown_server.sh
sleep 120
cd $IROOTDIR

