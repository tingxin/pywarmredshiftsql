from sqlglot import parse_one, exp

_SKIP_STR = [item.lower() for item in [
    'padb_fetch_sample',
    'from pg_catalog',
    'STL_',
    'SYS_',
    'SVV_',
    'STV_',
    'COPY',
    'pg_internal',
    'CREATE TEMPORARY TABLE',
    'create temp table',
    'CREATE TABLE']
            ]

def _check_query(query_str):
    if len(query_str.split(' ')) < 4:
        return False    
    for skip in _SKIP_STR:
        if query_str.find(skip) >= 0:
            return False
    return True

def analyse_one(sql: str) -> str:
    try:
        parser = parse_one(sql)
    except Exception as ex:
        return ""

    format_sql = str(parser)
    parts = list()
    index = 1
    for column in parser.find_all(exp.Where):
        for item in column.find_all(exp.Condition):
            if isinstance(item, exp.Literal):
                p = str(item.parent)
                part = p.replace(str(item), f"${index}")
                index += 1
                parts.append((p, part))

    for item in parts:
        format_sql = format_sql.replace(item[0], item[1])

    return format_sql

def analyse(sqls:list)->dict:
    warm_query_cache = dict()
    for item in sqls:
        query_text = item[0].strip().lower()
        if not _check_query(query_text):
            continue
            
        query_patten = analyse_one(query_text)
        if query_patten == "":
            continue
        
        if query_patten in warm_query_cache:
            continue

        warm_query_cache[query_patten] = query_text
    return warm_query_cache
