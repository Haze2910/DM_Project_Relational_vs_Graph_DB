from sqlalchemy.dialects.postgresql import insert

def upsert(table, conn, keys, data_iter):
    stmt = insert(table.table).values(list(data_iter))
    stmt = stmt.on_conflict_do_nothing()
    conn.execute(stmt)