from sqlglot import parse_one, exp

def run(sql: str) -> str:
    parser = parse_one(sql)
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
