#!/usr/bin/python3
import requests, os, sys, re, subprocess, sqlite3
import pygit2 as git
from itertools import groupby, chain
from concurrent.futures import ProcessPoolExecutor, as_completed

HASH_PATTERN = re.compile(r"Git-commit:[ \t]*([0-9a-f]{9,40})[ \t]*")
CMD = f'log --oneline --raw --no-merges'
BRANCHES_CONF = 'https://kerncvs.suse.de/branches.conf'
BRANCH_BLACKLIST = ['vanilla', 'linux-next']
DB_NAME = 'changes.sqlite'
BIG_BANG = '1da177e4c3f41524e886b7f1b8a0c1fc7321cac2'
BLACKLIST = 'Dell Inc.,XPS 13 9300' # weird file with spaces nobody gives a fig about

# db ###########################################################################

def create_db():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS branches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        tag_id INTEGER NOT NULL,
        FOREIGN KEY (tag_id) REFERENCES tags(id)
        );

        CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);

        CREATE TABLE IF NOT EXISTS commits (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);

        CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);

        CREATE TABLE IF NOT EXISTS backports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        commit_id INTEGER NOT NULL,
        branch_id INTEGER NOT NULL,
        FOREIGN KEY (commit_id) REFERENCES commits(id),
        FOREIGN KEY (branch_id) REFERENCES branches(id)
        );

        CREATE TABLE IF NOT EXISTS changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        score INTEGER,
        commit_id INTEGER NOT NULL,
        from_id INTEGER,
        to_id INTEGER,
        tag_id INTEGER,
        FOREIGN KEY (commit_id) REFERENCES commits(id),
        FOREIGN KEY (from_id) REFERENCES files(id),
        FOREIGN KEY (to_id) REFERENCES files(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
        );

        CREATE VIEW IF NOT EXISTS base_added AS
        SELECT b.name AS branch, f.name AS file, ci.name AS sha
        FROM files f
        JOIN changes ch ON ch.to_id = f.id AND ch.from_id IS NULL
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN tags t ON t.id = ch.tag_id
        JOIN branches b ON b.tag_id = t.id
        ;

        CREATE VIEW IF NOT EXISTS base_renamed AS
        SELECT b.name AS branch, f.name AS file, nf.name AS new_file, ci.name AS sha
        FROM files f
        JOIN changes ch ON ch.to_id IS NOT NULL AND ch.from_id = f.id
        JOIN files nf ON ch.to_id = nf.id
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN tags t ON t.id = ch.tag_id
        JOIN branches b ON b.tag_id = t.id
        ;

        CREATE VIEW IF NOT EXISTS base_removed AS
        SELECT b.name AS branch, f.name AS file, ci.name AS sha
        FROM files f
        JOIN changes ch ON ch.to_id IS NULL AND ch.from_id = f.id
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN tags t ON t.id = ch.tag_id
        JOIN branches b ON b.tag_id = t.id
        ;

        CREATE VIEW backports_added AS
        SELECT b.name AS branch, f.name AS file, ci.name AS sha
        FROM backports bp
        JOIN branches b ON b.id = bp.branch_id
        JOIN changes ch ON ch.commit_id = bp.commit_id
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN files f ON f.id = ch.from_id AND ch.to_id IS NULL
        ;

        CREATE VIEW backports_renamed AS
        SELECT b.name AS branch, f.name AS file, nf.name AS new_file, ci.name AS sha
        FROM backports bp
        JOIN branches b ON b.id = bp.branch_id
        JOIN changes ch ON ch.commit_id = bp.commit_id
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN files f ON f.id = ch.from_id AND ch.to_id IS NOT NULL
        JOIN files nf ON ch.to_id = nf.id
        ;

        CREATE VIEW backports_removed AS
        SELECT b.name AS branch, f.name AS file, ci.name AS sha
        FROM backports bp
        JOIN branches b ON b.id = bp.branch_id
        JOIN changes ch ON ch.commit_id = bp.commit_id
        JOIN commits ci ON ci.id = ch.commit_id
        JOIN files f ON f.id = ch.from_id AND ch.to_id IS NULL
        ;

        CREATE VIEW added AS
        SELECT branch, file, sha, 'base' AS source FROM base_added
        UNION ALL
        SELECT branch, file, sha, 'backport' AS source FROM backports_added
        ;

        CREATE VIEW renamed AS
        SELECT branch, file, new_file, sha, 'base' AS source FROM base_renamed
        UNION ALL
        SELECT branch, file, new_file, sha, 'backport' AS source FROM backports_renamed
        ;

        CREATE VIEW removed AS
        SELECT branch, file, sha, 'base' AS source FROM base_removed
        UNION ALL
        SELECT branch, file, sha, 'backport' AS source FROM backports_removed
        ;
        ''')

def store_array_into_db(query, array):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        cursor.executemany(query, array)
        conn.commit()

def get_commits():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        return { x for (x,) in cursor.execute('SELECT name FROM commits;') }

def store_tags_into_db(uniq_tags):
    many = [(t,) for t in uniq_tags]
    query = 'insert into tags (name) VALUES (?)'
    store_array_into_db(query, many)

def store_branches_into_db(tags):
    many = [(k, f'v{v}') for k, v in tags.items()]
    query = 'insert into branches (name, tag_id) VALUES (?, (select id from tags where name = ?))'
    store_array_into_db(query, many)

def store_commits_into_db(many):
    query = 'insert into commits (name) VALUES (?)'
    store_array_into_db(query, many)

def store_files_into_db(many):
    query = 'insert or ignore into files (name) VALUES (?)'
    store_array_into_db(query, many)

def store_changes_into_db(many):
    query = '''insert into changes (commit_id, score, from_id, to_id, tag_id) VALUES (
    (select id from commits where name = ?),
    ?,
    (select id from files where name = ?),
    (select id from files where name = ?),
    (select id from tags where name = ?)
    )'''
    store_array_into_db(query, many)

def store_backports_into_db(many):
    query = '''insert into backports (commit_id, branch_id) VALUES (
    (select id from commits where name = ?),
    (select id from branches where name = ?)
    )'''
    store_array_into_db(query, many)

# compare two references #######################################################

def get_renames_with_score_or_none(l):
    rr = l.split(' ')[4].split('\t')
    if rr[0].startswith('R'):
        return (rr[0], rr[1], rr[2])
    if rr[0].startswith('A'):
        return ('add', rr[1])
    if rr[0].startswith('D'):
        return ('del', rr[1])
    return None

def get_hash(l, repo):
    return repo.revparse_single(l.split(' ')[0]).id

def between(begin, end, lpath):
    many = []
    core_cmd = f'git -C {lpath} ' + CMD
    if begin:
        cmd = f'{core_cmd} {begin}..{end}'
    else:
        cmd = f'{core_cmd} {end}'
    # debug
    print(cmd)
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    lrepo = git.Repository(lpath)
    hash = None
    for l in res.stdout.decode('utf-8', 'ignore').split('\n'):
        if not l or l.startswith(BIG_BANG[:12]):
            break
        if BLACKLIST in l:
            continue
        if l.startswith(':'):
            r = get_renames_with_score_or_none(l)
            if r and len(r) == 3:
                many.append((str(hash), int(r[0][1:]), r[1], r[2], end))
            elif r and len(r) == 2:
                if r[0] == 'add':
                    many.append((str(hash), None, None, r[1], end))
                elif r[0] == 'del':
                    many.append((str(hash), None, r[1], None, end))
        else:
            hash = get_hash(l, lrepo)
    commits = {(h,) for h, _, _, _, _ in many }
    store_commits_into_db(list(commits))
    filesx = [f for _, _, f, _, _ in many if f ]
    filesy = [f for _, _, _, f, _ in many ]
    files = {(f,) for f, _ in groupby(chain(filesx, filesy))}
    return (files, many)

# get tags and branches ########################################################

def fetch_branches_conf():
    try:
        data = requests.get(BRANCHES_CONF)
        return data.text
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}", file=sys.stderr)
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}", file=sys.stderr)
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}", file=sys.stderr)
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}", file=sys.stderr)
    return None

def get_list_of_branches(branches_conf):
    ret = []
    for l in branches_conf.split('\n'):
        if not l or l.startswith('#') or l.startswith(' ') or ':' not in l or 'build' not in l:
            continue
        branch_name = l.split(':')[0]
        if branch_name in BRANCH_BLACKLIST:
            continue
        ret.append(branch_name)
    return ret

def key_function(s):
    arr = re.split(r'[.-]', s)[:3]
    arr[0] = int(arr[0])
    if len(arr) > 1:
        arr[1] = int(arr[1])
    if len(arr) > 2:
        if arr[2].startswith('rc'):
            arr[2] = int(arr[2][2:])
        else:
            arr[2] = int(arr[2])
    return arr

def extract_srcversion(content):
    for l in content.split('\n'):
        if l.startswith('SRCVERSION='):
            return l.split('=')[1]
    return ''

def get_tags_from_ksource_tree(branches, repo):
    ret = {}
    for b in branches:
        try:
            tree = repo.revparse_single('origin/' + b).tree
            entry = tree['rpm/config.sh']
            blob = repo[entry.id]
            content = blob.data.decode('utf-8', 'ignore')
            ret[b] = extract_srcversion(content)
        except KeyError:
            print('#', b, file=sys.stderr)
    return ret

def get_hash_or_nothing(fc, lrepo):
    for l in fc.split('\n'):
        ps = re.findall(HASH_PATTERN, l)
        if ps:
            try:
                return str(lrepo.revparse_single(ps[0]).id)
            except:
                return None
    return None

def get_commits_per_branch(branches, krepo, lrepo):
    ret = {}
    for b in branches:
        ret[b] = []
        try:
            tree = krepo.revparse_single('origin/' + b).tree
            tree_index = git.Index()
            tree_index.read_tree(tree)
            patches_ids = [ t.id for t in tree_index if t.path.startswith('patches.suse/') ]
            for pid in patches_ids:
                hs = get_hash_or_nothing(krepo[pid].data.decode('utf8', 'ignore'), lrepo)
                if hs:
                    ret[b].append(hs)
        except Exception as e:
            print('#', b, e, file=sys.stderr)
    return ret

def fetch_root_tree_files(lrepo, tag):
    tree_index = git.Index()
    try:
        tree_index.read_tree(lrepo.revparse_single(BIG_BANG).tree)
    except Exception as e:
        print(color_format(T_RED, f'branch {branch} probably does not exist: {e}'), file=sys.stderr)
        sys.exit(1)
    store_files_into_db([ (e.path,) for e in tree_index ])
    store_commits_into_db([(BIG_BANG,)])
    store_changes_into_db([ (BIG_BANG, None, None, e.path, tag) for e in tree_index ])

def prepare_tags_for_parallel_partition(uniq_tags):
    return [('', uniq_tags[0])] + [ (f, s) for f, s in zip(uniq_tags, uniq_tags[1:]) ]

# main function ################################################################

def main():
    os.path.isfile(DB_NAME) and os.rename(DB_NAME, DB_NAME + '.OLD')
    create_db()

    branches_conf = fetch_branches_conf()
    branches = []
    if branches_conf:
        branches = get_list_of_branches(branches_conf)

    kpath = os.getenv('KSOURCE_GIT', None)
    if not kpath:
        print("Cannot get KSOURCE_GIT", file=sys.stderr)
    krepo = git.Repository(kpath)
    tags = get_tags_from_ksource_tree(branches, krepo)

    pure_tags = sorted([ tags[k] for k in tags.keys() ], key=key_function)
    pure_tags = [ 'v' + p for p in pure_tags ]
    uniq_tags = [ t for t, _ in groupby(pure_tags) ]

    store_tags_into_db(uniq_tags)
    uniq_tags.append('master')
    store_branches_into_db(tags)

    lpath = os.getenv('LINUX_GIT', None)
    if not lpath:
        print("Cannot get LINUX_GIT", file=sys.stderr)
    lrepo = git.Repository(lpath)
    fetch_root_tree_files(lrepo, uniq_tags[0])

    tag_pairs = prepare_tags_for_parallel_partition(uniq_tags)

    with ProcessPoolExecutor() as executor:
        futures = { executor.submit(between, first, second, lpath) for first, second in tag_pairs }
        for f in as_completed(futures):
            files, many = f.result()
            store_files_into_db(files)
            store_changes_into_db(many)

    commits_per_branch = get_commits_per_branch(branches, krepo, lrepo)
    commits = get_commits()
    for branch, hashes in commits_per_branch.items():
        backports = [(h, branch) for h in hashes if h in commits ]
        store_backports_into_db(backports)

main()
