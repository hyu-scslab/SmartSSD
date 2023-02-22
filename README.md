# Environment

OS: Ubuntu 20.04 LTS

kernel: 5.4.0-131-generic

# Prepare

1. Initiate submodules
```
git submodule init
git submodule update
```

2. Install packages
- Install generic packages
	```
	sudo apt-get update  
	sudo apt-get install -y gcc python3 python3-pip python-dateutil python3-dateutil bash sudo git gnuplot bison flex libreadline6 linux-tools-generic zlib1g-dev vim autoconf linux-tools-`uname -r` build-essential libreadline-dev pkg-config libmysqlclient-dev libpq-dev libtool
	```
- Download XRT packages
	- Access https://www.xilinx.com/support/download/index.html/content/xilinx/en/downloadNav/alveo/smartssd.htm (member registration is required.)

	- Select the platform and operating system version like below:
	<img width="1058" alt="image" src="https://user-images.githubusercontent.com/46515826/220605710-d776b03b-87cf-490a-b6ce-2b1d3267b0ac.png">

	- Download XRP runtime and target platform packages from that page and install them
		```
		$ sudo apt-get install xrt_202020.2.9.317_20.04-amd64-xrt.deb
		$ tar -xvf xilinx-u2-gen3x4-xdma-gc-platform-1-2_all.deb.tar.gz
		$ sudo apt-get install xilinx-u2-gen3x4-xdma-gc-base_1-3085628_all.deb  xilinx-u2-gen3x4-xdma-gc-validate_1-3085628_all.deb
		$ sudo apt-get install xilinx-u2-gen3x4-xdma-gc-1-202020-1-dev_1-3085628_all.deb
		```

	- Make the xrp package aware of the attached smartssd.
		```
		$ sudo /opt/xilinx/xrt/bin/xbmgmt flash --update --shell xilinx_u2_gen3x4_xdma_gc_base_1
		```

	- Cold reboot
	- Check if it is properly recognized through the `xbutil list` command
		```
		$ cd /opt/xilinx/xrt/lib
		$ source setup.sh
		$ xbutil list
		```
		The mesesage `INFO: Found total 1 card(s), 1 are usable` should be seen.

# PostgreSQL
```
cd PostgreSQL_with_AIDE
python3 selectivity.py \
	--run-mode=1 \
	--install \
	--initdb \
	--tables={$NUM_TABLES} \
	--table_size={$TABLE_SIZE} \
	--join-tbles={$NUM_JOINED_TABLES} \
	--threads={$NUM_OLTP_WORKER} \
	--time-oltp-only={$OLTP_ONLY_TIME} \
	--time-olap-only={$OLAP_ONLY_TIME} \
	--time={$TIME} \
	--num-short-olap={$NUM_SHORT_OLAP_WORKERS} \
	--num-long-olap={$NUM_LONG_OLAP_WORKERS}
```
- For testing help: `python3 selectivity.py --help`

# MyRocks
```
cd MyRocks_with_AIDE
python3 selectivity.py \
	--run-mode=1 \
	--install \
	--initdb \
	--tables={$NUM_TABLES} \
	--table_size={$TABLE_SIZE} \
	--join-tbles={$NUM_JOINED_TABLES} \
	--threads={$NUM_OLTP_WORKER} \
	--time-oltp-only={$OLTP_ONLY_TIME} \
	--time-olap-only={$OLAP_ONLY_TIME} \
	--time={$TIME} \
	--num-short-olap={$NUM_SHORT_OLAP_WORKERS} \
	--num-long-olap={$NUM_LONG_OLAP_WORKERS}
```
- For testing help: `python3 selectivity.py --help`
