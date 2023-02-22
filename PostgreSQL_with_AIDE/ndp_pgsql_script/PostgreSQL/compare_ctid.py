import struct

relations=[16402, 16414, 16487, 16423, 16429, 16426, 16420, 16493, 16490]
#relations=[16402, 16414, 16423]
relations.sort()

for relation in relations:
    ctids = []
    with open("data/{}.0.i".format(relation), "rb") as f:
        num_ctids = struct.unpack('i', f.read(4))
        while True:
            ctid = f.read(9)
            if ctid:
                ctids.append(ctid)
            else:
                break

    orig_ctids = []
    with open("data/base/16401/{}.ctids".format(relation), "rb") as f:
        if not f:
            continue

        num_ctids = struct.unpack('i', f.read(4))
        while True:
            ctid = f.read(9)
            if ctid:
                orig_ctids.append(ctid)
            else:
                break

    ctids.sort()
    orig_ctids.sort()

    fail = False
    if len(ctids) != len(orig_ctids):
        print("[{}] (FAIL) number of ctids doesn't match".format(relation))
        fail = True

    if fail:
        continue

    for i in range(len(ctids)):
        if ctids[i] != orig_ctids[i]:
            print("[{}] (FAIL) ctid don't match: idx = {}".format(relation, i))
            #print("\t{} || {}".format(ctids[i].hex(), orig_ctids[i].hex()))
            print(ctids[i])
            print(orig_ctids[i])
            print()
            fail = True
            break

    if fail:
        continue

    print("[{}] (SUCCESS) everything matches".format(relation))

