import os
import sys
import time

def get_chbench_db_size(path):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)

    return total


def get_stock_size(path, relid):
    total = 0
    for (root, directories, files) in os.walk(path):
        for file in files:
            if str(relid) in file:
                total += os.path.getsize(os.path.join(root, file))

    return total

if __name__=="__main__":
    curdir = os.getcwd()
    db_path = curdir+ "/../inst/data/.rocksdb/"
    stock_oid = 16420
    sec = 0
    max_time = int(os.environ['DBDURATION'])
    query_num = int(sys.argv[1])
    IS_AIDE = sys.argv[2]
    output_dir = sys.argv[3]

    f = open(output_dir + f"/file_size.log", 'w')
    AIDE = False

    if IS_AIDE == "true":
        AIDE = True

    while True:
        if sec > max_time:
            break

        db_size = get_chbench_db_size(db_path)
        stock_table_size = get_stock_size(db_path, stock_oid)
        
        f.write(f'{sec} {db_size} {stock_table_size} \n')
        f.flush()

        time.sleep(1)    
        sec += 1

    f.close()
        

