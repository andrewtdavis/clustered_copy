#!/usr/bin/env python3
import argparse, os, sys, threading, queue, hashlib, time

def relpath(root, p):
    r = os.path.relpath(p, root)
    return "." if r == "." else r

def open_outputs(out_manifest, outdir, shards):
    if shards > 0:
        os.makedirs(outdir, exist_ok=True)
        outs = [open(os.path.join(outdir, f"shard_{i:05d}.nul"), "ab", buffering=1024*1024)
                for i in range(shards)]
    else:
        os.makedirs(os.path.dirname(os.path.abspath(out_manifest)) or ".", exist_ok=True)
        outs = [open(out_manifest, "ab", buffering=1024*1024)]
    return outs

def main():
    ap = argparse.ArgumentParser(
        description="Two-phase dir-first parallel crawler for huge trees (NUL-separated, relative paths)."
    )
    ap.add_argument("--root", required=True, help="Source root (absolute path recommended)")
    ap.add_argument("--dirs-out", default="dirs.nul", help="Where to write discovered directories (NUL-separated, relative)")
    ap.add_argument("--out-manifest", help="Single output file manifest (NUL-separated, relative)")
    ap.add_argument("--outdir", help="Output directory for shard files (with --shards)")
    ap.add_argument("--shards", type=int, default=0, help="Write directly into this many shard files")
    ap.add_argument("--mode", choices=["roundrobin","hash","bydir"], default="bydir",
                    help="Shard selection (hash of full path, roundrobin, or directory-based)")
    ap.add_argument("--workers1", type=int, default=32, help="Threads for PHASE 1 (directory discovery)")
    ap.add_argument("--workers2", type=int, default=64, help="Threads for PHASE 2 (file listing per dir)")
    ap.add_argument("--maxq", type=int, default=200000, help="Queue size/backpressure")
    ap.add_argument("--exclude", action="append", default=[], help="Exclude directory names (repeatable)")
    ap.add_argument("--follow-symlinks", action="store_true", help="Follow directory symlinks (default: no)")
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        print(f"Root is not a directory: {root}", file=sys.stderr); sys.exit(2)

    if args.shards == 0 and not args.out_manifest:
        print("Provide --out-manifest, or use --outdir with --shards.", file=sys.stderr); sys.exit(2)
    if args.shards > 0 and not args.outdir:
        print("--outdir is required when using --shards", file=sys.stderr); sys.exit(2)

    # -------------------
    # PHASE 1: enumerate ALL directories (breadth-first-ish, parallel)
    # -------------------
    dir_q = queue.Queue(maxsize=args.maxq)
    stop = object()
    dir_out_lock = threading.Lock()
    dirs_emitted = 0
    dirs_file = open(args.dirs_out, "ab", buffering=1024*1024)

    def emit_dir(rel_dir):
        nonlocal dirs_emitted
        with dir_out_lock:
            dirs_file.write(rel_dir.encode("utf-8", "surrogatepass") + b"\0")
            dirs_emitted += 1
            if (dirs_emitted & 0xFFFF) == 0:
                dirs_file.flush()

    # seed with root, represented as '.' (so Phase 2 handles files at root)
    emit_dir(".")
    dir_q.put(root)

    def dir_worker():
        follow = args.follow_symlinks
        while True:
            d = dir_q.get()
            if d is stop:
                dir_q.task_done(); return
            try:
                with os.scandir(d) as it:
                    for de in it:
                        try:
                            if de.is_dir(follow_symlinks=follow):
                                if de.name in args.exclude:
                                    continue
                                emit_dir(relpath(root, de.path))
                                dir_q.put(de.path)
                        except (PermissionError, OSError):
                            continue
            except (PermissionError, OSError):
                pass
            finally:
                dir_q.task_done()

    t0 = time.time()
    ths1 = [threading.Thread(target=dir_worker, daemon=True) for _ in range(max(1, args.workers1))]
    for t in ths1: t.start()
    dir_q.join()
    for _ in ths1: dir_q.put(stop)
    for t in ths1: t.join()
    dirs_file.flush(); dirs_file.close()
    t1 = time.time()
    print(f"[PHASE1] dirs: {dirs_emitted} in {t1-t0:.1f}s", file=sys.stderr)

    # -------------------
    # PHASE 2: list files per directory in parallel, write manifest/shards
    # -------------------
    outs = open_outputs(args.out_manifest, args.outdir, args.shards)
    out_locks = [threading.Lock() for _ in outs]
    rr_counter = 0
    rr_lock = threading.Lock()
    files_emitted = 0
    files_lock = threading.Lock()

    def choose_out(full_rel_path, dir_rel):
        nonlocal rr_counter
        if len(outs) == 1:
            return 0
        if args.mode == "roundrobin":
            with rr_lock:
                i = rr_counter
                rr_counter += 1
            return i % len(outs)
        if args.mode == "bydir":
            # stable shard per directory (keeps locality)
            h = hashlib.blake2b(dir_rel.encode("utf-8", "surrogatepass"), digest_size=4).digest()
        else:  # "hash" (full relative path)
            h = hashlib.blake2b(full_rel_path.encode("utf-8", "surrogatepass"), digest_size=4).digest()
        return int.from_bytes(h, "little") % len(outs)

    dir_in_q = queue.Queue(maxsize=args.maxq)

    def file_worker():
        nonlocal files_emitted
        while True:
            dir_rel = dir_in_q.get()
            if dir_rel is stop:
                dir_in_q.task_done(); return
            dir_abs = root if dir_rel == "." else os.path.join(root, dir_rel)
            try:
                with os.scandir(dir_abs) as it:
                    for de in it:
                        try:
                            if de.is_file(follow_symlinks=False):
                                # build RELATIVE file path
                                if dir_rel == ".":
                                    rel_file = de.name
                                else:
                                    rel_file = dir_rel + os.sep + de.name
                                idx = choose_out(rel_file, dir_rel)
                                b = rel_file.encode("utf-8", "surrogatepass") + b"\0"
                                with out_locks[idx]:
                                    outs[idx].write(b)
                                with files_lock:
                                    files_emitted += 1
                                    if (files_emitted & 0xFFFFF) == 0:  # ~every 1M files
                                        for i, o in enumerate(outs):
                                            with out_locks[i]:
                                                o.flush()
                        except (PermissionError, OSError):
                            continue
            except (PermissionError, OSError):
                pass
            finally:
                dir_in_q.task_done()

    ths2 = [threading.Thread(target=file_worker, daemon=True) for _ in range(max(1, args.workers2))]
    for t in ths2: t.start()

    # stream directories from the Phase-1 file to avoid holding them all in RAM
    with open(args.dirs_out, "rb") as f:
        buf = bytearray()
        while True:
            chunk = f.read(1024*1024)
            if not chunk:
                break
            buf += chunk
            start = 0
            while True:
                try:
                    i = buf.index(0, start)
                except ValueError:
                    buf = buf[start:]
                    break
                dir_rel = buf[start:i].decode("utf-8", "surrogatepass") or "."
                dir_in_q.put(dir_rel)
                start = i + 1

    dir_in_q.join()
    for _ in ths2: dir_in_q.put(stop)
    for t in ths2: t.join()
    for i, o in enumerate(outs):
        with out_locks[i]:
            o.flush(); o.close()
    t2 = time.time()
    print(f"[PHASE2] files: {files_emitted} in {t2-t1:.1f}s ({(files_emitted/(t2-t1)) if (t2-t1)>0 else 0:.0f} files/s)",
          file=sys.stderr)

if __name__ == "__main__":
    main()